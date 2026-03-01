# FAQ for Zeytin Database Engine

Here, JeaFriday (Hi, that's me!!) answers some very confusing architectural choices and questions from you. Happy reading!

- "Is starting a private Isolate for every user efficient?"

No. Besides, Zeytin doesn't work that way anyway. Zeytin can work in two ways, and this changes completely depending on how you use Zeytin;

1. If you run Zeytin once locally, Zeytin runs only 1 Isolate. It uses the Isolate (referred to as a Truck in Zeytin literature) like the main system.

2. Multi-tenant: Each Truck you start uses an Isolate. At this point, Truck can be thought of as Project. Additionally, the engine only supports 50 Trucks. More wouldn't theoretically cause problems, but practically, since we don't know how much the behavior of the Zeytin engine would change, I chose to limit this during the development phase.

- "Zeytin loads data into RAM to keep it in cache. Won't this consume RAM after a certain point?"

No, because contrary to popular belief, Zeytin does not cache the data in RAM. Zeytin caches the address of the data, not the data itself: `(Key (Tag) + Offset (int) + Length (int))`. This takes up a very small space in RAM. Even with millions of data entries, it occupies a negligible amount of space.

- "You use flush: true on every write. Won't this physically wear out the disk?"

Yes, it wears it out significantly. Especially if you are constantly repeating small operations, you might consider not doing this. This is not an architectural flaw; it is a conscious security layer. By keeping the disk constantly fresh, we force the system to guarantee data accuracy.

**What can you use?**

This is actually about the choice you make while writing your code. If you can, consider bulk inserts. You can add 10,000 data entries in a short time like 1 second. Think about this!
