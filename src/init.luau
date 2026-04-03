--!native
--!optimize 2

--[=[
	Channels are containers of buffered messages for pull-based event handling.

	They are fully typechecked and are designed to be ECS-friendly.

	Messages are stored in two sequences. Using .update() moves removes old messages from the old sequence and moves new messages into a new sequence.
	To avoid memory leaks, you should periodically use .update().

	Usage example:
	```luau
		local example = channel.new<<number?>>()
		local reader = channel.reader_new(example)
		local writer = channel.writer_new(example)

		-- To write messages into a channel, you can use a message writer.
		-- Output: 1
		print(writer:write(example, 10))

		-- Messages can be nil.
		-- Output: 2
		print(writer:write(example, nil))

		-- To iterate through messages, you can use a message reader, it can also be reused after iteration.
		-- For faster iteration, use reader:read_fast().
		--[[ Output:
			1 1
			nil 2
		]]
		for msg, id in reader do
			print(msg, id)
		end
	```
]=]
local mod = {}

local writer_scope = {}
local reader_scope = {}

--- Represents the ID of a message
export type MessageId = number
--- Represents a full message instance with the message itself and message ID
export type MessageInstance<T> = { read message: T, read id: MessageId }
--- Represents a sequence of message instances and oldest message ID
export type MessageSequence<T> = { read messages: { MessageInstance<T> }, message_start_count: number }

--[=[
	Channels are containers of buffered messages for pull-based event handling.

	They are fully typechecked and are designed to be ECS-friendly.
]=]
export type Channel<T> = {
	messages_a: MessageSequence<T>,
	messages_b: MessageSequence<T>,
	message_count: number,
}

--[=[
	Abstracts writing messages to a channel.
]=]
export type ChannelWriter<T> = setmetatable<{ read messages: Channel<T> }, { __index: typeof(writer_scope) }>
--- Provides a way for iteration over a channel's messages.
export type ChannelReader<T> = setmetatable<
	{ read messages: Channel<T>, last_message_count: number },
	{ __index: typeof(reader_scope), __iter: (self: any) -> () -> (T, MessageId) }
>

local function channel_reset_count(self: Channel<any>): ()
	local count: number = self.message_count
	self.messages_b.message_start_count = count + 1
	self.messages_a.message_start_count = count + 1
end

local function channel_get_oldest_id(self: Channel<any>): number
	return self.messages_a.message_start_count
end

local function channel_find_sequence<T>(self: Channel<T>, id: number): MessageSequence<T>?
	return if id < channel_get_oldest_id(self)
		then nil
		else if id < self.messages_b.message_start_count then self.messages_a else self.messages_b
end

--- Returns the number of messages a channel has,
--- @return Number of messages.
local function channel_len(self: Channel<any>): number
	return #self.messages_a.messages + #self.messages_b.messages
end

--- Checks if a channel has any messages.
--- @return Boolean which indicates if the channel is empty.
local function channel_is_empty(self: Channel<any>): boolean
	return channel_len(self) == 0
end

--- Deletes all messages from a channel.
local function channel_clear(self: Channel<any>): ()
	channel_reset_count(self)
	table.clear(self.messages_a.messages)
	table.clear(self.messages_b.messages)
end

--- Writes a message into a channel.
--- @param message -- message to write.
--- @return Message ID of the written message.
local function channel_write<T>(self: Channel<T>, message: T): MessageId
	self.message_count += 1
	local id: MessageId = self.message_count
	local message_ins: MessageInstance<T> = { message = message, id = id }
	table.insert(self.messages_b.messages, message_ins)
	return id
end

--- Writes messages from a table into a channel.
--- @param messages -- table of messages to write.
--- @return Message ID of the first written message.
local function channel_write_batch<T>(self: Channel<T>, messages: { T }): MessageId
	local count: number = self.message_count
	local base_id: MessageId = count
	for _, message: T in messages do
		count += 1
		local message_ins: MessageInstance<T> = { message = message, id = count }
		table.insert(self.messages_b.messages, message_ins)
	end
	self.message_count += #messages
	return base_id
end

--- Deletes old messages from a channel and moves new messages to old ones.
local function channel_update(self: Channel<any>): ()
	self.messages_a, self.messages_b = self.messages_b, self.messages_a
	table.clear(self.messages_b.messages)
	self.messages_b.message_start_count = self.message_count
end

--- Drains old messages from the channel, collecting them into a table.
--- @return Table of old drained messages.
local function channel_update_drain<T>(self: Channel<T>): { MessageInstance<T> }
	self.messages_a, self.messages_b = self.messages_b, self.messages_a

	local drained: { T } = table.clone(self.messages_b.messages)

	table.clear(self.messages_b.messages)
	self.messages_b.message_start_count = self.message_count

	return drained
end

--- Drains all messages into a table, removing them from the channel.
--- @return Table of all messages.
local function channel_drain<T>(self: Channel<T>): { MessageInstance<T> }
	channel_reset_count(self)

	local drained: { MessageInstance<T> } = table.clone(self.messages_a.messages)
	for _, msg: MessageInstance<T> in self.messages_b.messages do
		table.insert(drained, msg)
	end
	table.clear(self.messages_a.messages)
	table.clear(self.messages_b.messages)

	return drained
end

local function channel_get_new_messages<T>(self: Channel<T>): { MessageInstance<T> }
	return self.messages_b.messages
end

local function channel_get_old_messages<T>(self: Channel<T>): { MessageInstance<T> }
	return self.messages_a.messages
end

--- Returns a message instance under a specific ID.
--- @param id -- ID to look for.
--- @return Message instance if found.
local function channel_get_message<T>(self: Channel<T>, id: number): MessageInstance<T>?
	local sequence: MessageSequence<T>? = channel_find_sequence(self, id)
	if not sequence then return nil end
	local message_ins: MessageInstance<T> =
		sequence.messages[math.clamp(id - sequence.message_start_count + 1, 1, math.huge)]
	return message_ins
end

--- Creates a new empty channel.
--- @return Channel - Channel object
local function channel_new<T>(): Channel<T>
	return {
		messages_a = { messages = {} :: any, message_start_count = 0 },
		messages_b = { messages = {} :: any, message_start_count = 0 },
		message_count = 0,
	}
end

--- Returns the number of messages the reader can iterate over.
--- @return Number of messages.
local function reader_len<T>(self: ChannelReader<T>): number
	local messages: Channel<T> = self.messages
	return math.min(math.clamp(messages.message_count - self.last_message_count, 0, math.huge), channel_len(messages))
end

--- Returns the number of messages the reader has missed.
--- @return Number of missed messages.
local function reader_get_missed<T>(self: ChannelReader<T>): number
	return math.clamp(channel_get_oldest_id(self.messages) - self.last_message_count, 0, math.huge)
end

--- Returns a boolean that indicates if the reader has no messages to iterate over.
--- @return Boolean indicator.
local function reader_is_empty<T>(self: ChannelReader<T>): boolean
	return reader_len(self) == 0
end

--- Resets the reader, basically removing all messages from iteration.
local function reader_clear<T>(self: ChannelReader<T>): ()
	self.last_message_count = self.messages.message_count
end

--- Writes a message to a channel via channel writer.
--- @param message -- message to write.
--- @return Message ID of the written message.
local function writer_write<T>(self: ChannelWriter<T>, message: T): MessageId
	return channel_write(self.messages, message)
end

--- Writes messages from a table to a channel via channel writer.
--- @param messages -- table to messages to write.
--- @return Message ID of the first written message.
local function writer_write_batch<T>(self: ChannelWriter<T>, messages: { T }): MessageId
	return channel_write_batch(self.messages, messages)
end

--- Creates a new channel writer.
--- @param messages -- channel to write into.
--- @return New channel writer.
local function writer_new<T>(messages: Channel<T>): ChannelWriter<T>
	return setmetatable({ messages = messages }, { __index = writer_scope })
end

local function reader_get_target<T>(messages: Channel<T>, last_message_count: number): ({ MessageInstance<T> }, number)
	local messages_a: MessageSequence<T>, messages_b: MessageSequence<T> = messages.messages_a, messages.messages_b
	local messages_a_messages: { T }, messages_b_messages: { T } = messages_a.messages, messages_b.messages
	local messages_a_size: number, messages_b_size: number = #messages_a_messages, #messages_b_messages

	local idx_a: number, idx_b: number =
		math.clamp(last_message_count - messages_a.message_start_count, 0, math.huge),
		math.clamp(last_message_count - messages_b.message_start_count, 0, math.huge)

	local a_slice_size: number = math.clamp(messages_a_size - idx_a, 0, math.huge)
	local b_slice_size: number = math.clamp(messages_b_size - idx_b, 0, math.huge)
	local total_size: number = a_slice_size + b_slice_size

	local target: { MessageInstance<T> } = table.create(total_size) :: any
	if a_slice_size > 0 then table.move(messages_a_messages, idx_a + 1, messages_a_size, 1, target) end
	if b_slice_size > 0 then table.move(messages_b_messages, idx_b + 1, messages_b_size, a_slice_size + 1, target) end
	return target, total_size
end

--- Returns an iterator for messages.
--- @return Message iterator.
local function reader_read<T>(self: ChannelReader<T>): () -> (T, MessageId)
	local missed: number = reader_get_missed(self)
	self.last_message_count += missed
	local target: { MessageInstance<T> } = reader_get_target(self.messages, self.last_message_count)
	local cached_size: number = self.messages.message_count
	return function(): (T, MessageId)
		local next_msg: MessageInstance<T> = target[#target - cached_size + self.last_message_count + 1]
		if next_msg then
			self.last_message_count += 1
			return next_msg.message, next_msg.id
		end
		return nil :: any, nil :: any
	end
end

--- Collects messages into a table instead of using an iterator function, providing better performance.
--- @return Table of messages for iteration.
local function reader_read_fast<T>(self: ChannelReader<T>): { MessageInstance<T> }
	local missed: number = reader_get_missed(self)
	self.last_message_count += missed
	local target: { MessageInstance<T> }, size: number = reader_get_target(self.messages, self.last_message_count)
	self.last_message_count += size
	return target
end

--- Creates a new channel reader from a channel.
--- @param channel -- channel to use as a source of messages.
--- @return New channel reader.
local function reader_new<T>(messages: Channel<T>): ChannelReader<T>
	return setmetatable({
		messages = messages,
		last_message_count = messages.message_count,
	}, {
		__index = reader_scope,
		__iter = reader_read,
	})
end

writer_scope.write = writer_write
writer_scope.write_batch = writer_write_batch
writer_scope.new = writer_new

reader_scope.len = reader_len
reader_scope.is_empty = reader_is_empty
reader_scope.cursor_get_missed = reader_get_missed
reader_scope.clear = reader_clear
reader_scope.iter = reader_read
reader_scope.read = reader_read_fast
reader_scope.read_fast = reader_read_fast
reader_scope.new = reader_new

mod._get_oldest_id = channel_get_oldest_id
mod._find_sequence = channel_find_sequence
mod.len = channel_len
mod.is_empty = channel_is_empty
mod.clear = channel_clear
mod.write = channel_write
mod.write_batch = channel_write_batch
mod.update = channel_update
mod.update_drain = channel_update_drain
mod.drain = channel_drain
mod.get_old_messages = channel_get_old_messages
mod.get_new_messages = channel_get_new_messages
mod.get_message = channel_get_message
mod.new = channel_new

mod.writer = writer_scope
mod.reader = reader_scope
mod.reader_new = reader_new
mod.writer_new = writer_new

return mod
