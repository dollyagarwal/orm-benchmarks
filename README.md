# orm-benchmarks

Object-relational mappers (ORMs) are becoming increasingly popular and important amongst the developer community because of their ease of use. Several programming languages offer a variety of different ORMs. In this paper, we introduce an approach for systematically testing different ORMs available in Python. We also compare the ORMs with the traditional native queries in Python. We propose a way to compare the different features offered by the ORMs as well as compare their performance against each other, using a well-designed test case suite. For the purpose of this experiment, we consider a simplified version of the TPC-C database. 

# Discussion
Based on all the tests run, these are some of the key takeaways:
    - Tortoise performs really well on large data tables because of the async feature, but for smaller tables, the cost of starting the async outweighs the benefits of it. 
    - Pony ORM performs better than other ORMs on filter queries.
    - SQLAlchemy ORM supports most of the features and also performs better or competitive to other ORMs for most of the use cases.
    - Django is a open-source web framework. One can not start using Django ORM without setting up web application. 
    - Peewee is a simple and small ORM but generally performance is slow when compared to other ORMs.


# Conclusion
Based on the results of this study, we conclude that no one ORM fits all use cases. However, we highlight where certain ORMs may be suitable over others. When the need is good performance on large tables, Tortoise might be the way to go, but in case the need is to have features closely resembling SQL features, then SQLAlchemy might be a better choice. Pony suits best for fast prototyping, thanks to its Entity-Relationship Diagram Editor and its pythonic style while Django might be the preferred choice for a web application framework. One can choose Peewee when working with only small tables but the requirement is to have all the features. After running all test cases, we realised that while native Python SQL queries have better performance, ORMs offer an easy way to work with relational databases, they do most of the work.

As part of the future work, ORMs can be tested on TPC-C benchmark to evaluate the speed of transaction processing when several virtual users access the database at the same time. This would be useful because most ORMs are selected for use in web applications where multiple users send all manner of queries to a database often at the same time. 
