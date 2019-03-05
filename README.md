# bigdata-integration
#UniversityofCincinnati

This repository is consist of code to enable Big Data Integration project of integrating a publicly available data source with cludera-training-vm data of dualcore dataset.

Below two were major bottlenecks during data integration process.
- Identify different records which represent the same item/product.
- Generate category field to perform cumulative analsis on product category.

Solutions:
Probabilstic String Matching: Identify semantics that represent the same thing based on probabilistic string matching technique, considering that they have same merchant, brand, weight and primary category .
(Note: It is neither hashbased nor word pronounce based algorithm. It is a navie attempt to match string based on words)

Cateogry generation: Generates category based on contents of name filed by searching relavant keywords in property file.
Splits name field into words and try to find words in the property file and maintain a count for each category value. Compares the highest and second highest count of values, if they match then the program doesnt update cateogry field for the specific product, otherwise it updates category field with the highest count value from property file.
