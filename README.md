Ruvos- N3C Long Covid Challenge
==============================
Authors: Jenny Blase & Jake Hightower, Ruvos LLC


BackGround/ Description
-----------------------

The N3C Data Enclave has provided a rich data history of electronic health records (EHR) for the prediction of post-acute sequelae of SARS-CoV-2 (PASC) in patients. Two major challenges of this data are: 1) the use of the Observational Medical Outcomes Partnership (OMOP) common data model can be particularly challenging to clean, organize, and create meaningful features for target prediction because of the layers of mapping between OMOP features and the clinical context by which they were recorded, and 2) EHR data is predominantly categorical, which when one-hot encoded, leads to large sparse matrices of features known for hampering model accuracy. We sought to overcome these obstacles by feature engineering meaningful condition categories, such that they were broad enough to capture the diversity of patient conditions but high enough in frequency to minimize sparsity. We aggregated conditions to the Clinical Classifications Software Refined v.2021.2 (CCSR) (1), used the Charlson Comorbidity Index (2), and incorporated patient demographics while still keeping the total feature space close to 200 inputs. Finally, we trained two machine learning models, XGBoost and a Feed Forward Deep Neural Network, and evaluated an ensemble voting algorithm to combine them. Ultimately we conclude that while the ensemble architecture is an intelligent design, in this study we did not find significant F1/AUC gain to merit the increased complexity over the XGBoost model alone.


Acknowledgments
---------------
The N3C Data Enclave has provided a rich data history of electronic health records (EHR) for the prediction of post-acute sequelae of SARS-CoV-2 (PASC) in patients. Two major challenges of this data are: 1) the use of the Observational Medical Outcomes Partnership (OMOP) common data model can be particularly challenging to clean, organize, and create meaningful features for target prediction because of the layers of mapping between OMOP features and the clinical context by which they were recorded, and 2) EHR data is predominantly categorical, which when one-hot encoded, leads to large sparse matrices of features known for hampering model accuracy. We sought to overcome these obstacles by feature engineering meaningful condition categories, such that they were broad enough to capture the diversity of patient conditions but high enough in frequency to minimize sparsity. We aggregated conditions to the Clinical Classifications Software Refined v.2021.2 (CCSR) (1), used the Charlson Comorbidity Index (2), and incorporated patient demographics while still keeping the total feature space close to 200 inputs. Finally, we trained two machine learning models, XGBoost and a Feed Forward Deep Neural Network, and evaluated an ensemble voting algorithm to combine them. Ultimately we conclude that while the ensemble architecture is an intelligent design, in this study we did not find significant F1/AUC gain to merit the increased complexity over the XGBoost model alone.

How To Use This
---------------

1. Run `pip install -r requirements.txt` to install dependencies
2a. For admin users in the N3C enclave, you can extract directly from the 'pipeline.py' file directly which contains all of the transforms to recreate our model.
2b. For users trying to run this independently through GitHub, note this model runs entirely on Python and assumes you have a PySpark environment set up, mirroring that of the N3C Enclave.
3. Navigate to /src and run all of 'final_pipeline.py'. Due to size limitations, the following data sets are not included in the /data directory and will need to be manually imported into the code, these sections are discussed and marked clearly in commented code at the top of the final_pipeline.py file. Datasets not included but required are:

     Enclave Name                      Python Script Naming Convention
# 1. concept                       --> concept_train & concept
# 2. condition_occurence           --> condition_occurence_train & condition_occurence
# 3. microvisits_to_macrovisits    --> microvisits_to_macrovisits_train & microvisits_to_macrovisits
# 4. observation                   --> observation_train & observation
# 5. procedure_occurence           --> procedure_occurence_train & procedure_occurence
 
