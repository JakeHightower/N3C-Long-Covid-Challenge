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
2. Run cohort creation, Tim Bergquist will make available synpuf synthetic OMOP data for repeatability
3. Run data_preparation.py (create OMAP Mappings & feature engineering)
4. Run model_prep.py
5. Run xgboost_hyperparam_tuning.py
6. Run ruvos_predictions.py
