# Patterns in Practice: A Journey Through Distributed Systems

I picked up "Patterns of Distributed Systems" by Unmesh Joshi. The book does a great job in demisifying some key concepts of ditributed systems in a practical way. I may have deluded myself into thinking i will have enough understadning of these concepts to develop a system that uses them. This repo will be reflective of my journey through the chapters of the book as i incrementally introduce the concpets i learn into the application "keyvault"

Upon completion, keyvault will be a hight performant distributed key value store running multiple instances/nodes. The ineraction of its nodes will be guided by the following patterns:

- Write Ahead Log
- Segmented Log
- Low-Water Mark
- Leader and Followers
- Heartbeat
- Majority Quorum
- Generation Clock
- Replicated Log
- SIngular Update queue

I chose to build a kew value store for this because it is data intensive and is the perfect candidate for seeing these concepts in action. Golang is the language of choice, not for any techincal reasons, but because it is just an opportunity for me to get better with it.
