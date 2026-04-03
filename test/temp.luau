local channels_mod = require("../src")

local example = channels_mod.new<<number?>>()
local reader = channels_mod.reader_new(example)
local writer = channels_mod.writer_new(example)

assert(writer:write(example, 10) == 1)

assert(writer:write(example, nil) == 2)

for msg, id in reader do
	assert(msg == 10 or msg == nil)
end
