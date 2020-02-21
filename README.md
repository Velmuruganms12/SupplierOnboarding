### Product Onboarding
Application for Onboarding new Supplier Product Data



![pipeline](https://github.com/Velmuruganms12/SupplierOnboarding/blob/master/Onboarding-DFD.jpg)

  #### Pre-Processing
  Read the input data from JSON and convert Input source attributes from Row to Columns, for matching with target data.
  
  
  #### Normalisation
  Color attribute translated to English for matching with target data.
  Make attribute Matched with target data by chaging the text to Title case.
  
  #### Extraction
  Unit and value extracted from ComsumptionTotalText attribute.
  
  
  #### Integration
  - Rename exist column & creating new column to match Target Schema.
  - Selecting Specific Column to Match Target Schema.
  
  #### Product Matching
  - Machine Learning & NLP needed to be implement for Modal & Modal Variant as text are in different pattern.
  - Identified new or Existing data based on following attributes - Make, Color and City.
  - Delta lake can be used to merge existing records and maintaining the version.
  


#### Compile the project:

$ sbt compile

Run the project:

$ sbt run







### Dependencies :
Spark Version : 2.4.0

Scala : 2.12.8

Spark-excel : 0.12.3

