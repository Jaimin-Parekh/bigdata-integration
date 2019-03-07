# bigdata-integration
#UniversityofCincinnati

This repository is consisting of code to enable Big Data Integration project of integrating a publicly available data source with cludera-training-vm data of dualcore dataset.

Below two were major bottlenecks during data integration process.
- Identify different records which represent the same item/product.
- Generate category field to perform cumulative analysis on the product category.

Solutions:
Probabilistic String Matching: Identify semantics that represents the same thing based on probabilistic string matching technique, considering that they have the same merchant, brand, weight and primary category.
(Note: It is neither hash-based nor word pronounces based algorithm. It is a naive attempt to match string based on words)

Category generation: Generates category based on contents of name filed by searching relevant keywords in the property file.
Splits name field into words and try to find words in the property file and maintain a count for each category value. Compares the highest and second highest count of values, if they match then the program doesnt update cateogry field for the specific product, otherwise it updates category field with the highest count value from property file.
