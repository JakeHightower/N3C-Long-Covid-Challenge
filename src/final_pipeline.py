#!/usr/bin/env python
# -*- coding: utf-8 -*-
#PEP8 best practices: https://peps.python.org/pep-0008/
"""
Title: Translation of Code Workbooks from N3C Enclave to External File on GitHub
Author(s): Jake Hightower & Jenny Blase
Last Updated: 4/12/2023
Summary: The following script can be used to locally run our N3C Long Covid Challenge Submission.

         **IMPORTANT**
         Data models were trained and tested using the following datasets:
             -concept
             -conept_set_members
             -condition_era
             -condition_occurence
             -drug_era
             -DXCCSR_v2021-2
             -long_covid_silver_standard
             -mapped_concepts
             -microvisits_to_macrovisits
             -observation
             -person
             -procedure_occurnce

         However, due to size limitations, only the following datasets are included in this repository:
             -concept_set_members
             -DXCCSR_v2021-2
             -condition
             -drug_era
             -long_covid_silver_standard
             -mapped_concepts
             -person

        The remainder will need to be manually updated, search <INSERT_ENCLAVE_DATA> to find in script


"""


#==============================================================================
# IMPORT LIBRARIES
#==============================================================================
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import Imputer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd
import numpy as np
import time
from hyperopt import STATUS_OK, Trials, fmin, hp, tpe
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score, confusion_matrix, roc_curve
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier, plot_importance
import matplotlib.pyplot as plt


#==============================================================================
# Define Functions
#==============================================================================

#-----------------------------
# Creat Mapped Concepts
#-----------------------------
def mapped_concepts(concept_relationship, concept, DXCCSR_v2021):

    try:
        #check if a mapped_concepts file exists
        mapped_concepts_file = spark.read.option("header",True).csv("../data/mapped_concepts.csv")

        return mapped_concepts_file
    
    except FileNotFoundError:
        #Grabbing all maps_to relationships between concepts where the map doesn't map to itself
        maps_to = concept_relationship.filter((concept_relationship.relationship_id=="Maps to") & (concept_relationship.concept_id_1!=concept_relationship.concept_id_2))

        #ICD10s for all conditions
        concept_icd10 = concept.filter((concept.domain_id=="Condition") & (concept.vocabulary_id=="ICD10CM") & (concept.invalid_reason.isNull()))
        concept_icd10 = concept_icd10.withColumn("concept_code_clean", F.regexp_replace("concept_code", "\.", ''))

        concept_icd10 = concept_icd10.alias('concept_icd10')
        maps_to = maps_to.alias('maps_to')

        #Joining concept_relationship and concept tables. This table has 120,696 rows.
        concept_map = maps_to.join(concept_icd10,concept_icd10.concept_id == maps_to.concept_id_1,'inner').select('concept_icd10.concept_id', 'concept_icd10.concept_name', 'concept_icd10.concept_code_clean', 'maps_to.concept_id_1', 'maps_to.concept_id_2')

        #Pulling in CCSR groupings, DXCCSR_v2021_2 data table has 73,211 rows
        ccsr = DXCCSR_v2021.withColumn("icd10cm_clean", F.regexp_replace("`ICD-10-CM_CODE`", "'", ''))
        ccsr = ccsr.withColumn("default_ccsr_category_op_clean", F.regexp_replace("Default_CCSR_CATEGORY_OP", "'", ''))

        #Final table has 125,203 rows. 26,723 rows (containing 20,724 unique icd10 codes) are in the concept table but not in the ccsr table
        return concept_map.join(ccsr, concept_map.concept_code_clean==ccsr.icd10cm_clean,'outer')

#-----------------------------
# Map Conditions
#-----------------------------
#Joining conditions to mapped concepts. This is a many to many match, icd_match code cleans it to make 1:1 mappings.
def condition_mapped(mapped_concepts, condition_era_train, condition_era):
    #38,044 patients have conditions in the condition_era_train table
    condition_train_test = condition_era_train.unionByName(condition_era, allowMissingColumns=True).dropDuplicates(['condition_era_id'])
    return condition_train_test.join(mapped_concepts, condition_train_test.condition_concept_id==mapped_concepts.concept_id_2,'left')



#-----------------------------
# ICD Matching
#-----------------------------

def icd_match(condition_mapped):
    #Recoding all pregnancy categories into 1 group since they are highly correlated. Adding some manual mappings for concepts that did not map directly to ICD10 codes.
    condition_mapped = condition_mapped.withColumn('default_ccsr_category_op_clean', F.when(F.col("default_ccsr_category_op_clean").startswith('PRG'), 'PRG').when(F.col("condition_concept_id")==4113821, 'MBD005').when(F.col("condition_concept_id")==4195384, 'SYM013').otherwise(F.col("default_ccsr_category_op_clean")))

    keep_vars = ["person_id", "condition_era_id", "condition_era_start_date", "icd10cm_clean", "default_ccsr_category_op_clean", "condition_concept_id", "condition_concept_name"]
    #Removing XXX111 (Unacceptable DX1) category, nulls and duplicates for CCSR
    df = condition_mapped.filter((condition_mapped.default_ccsr_category_op_clean.isNotNull()) & (condition_mapped.default_ccsr_category_op_clean!='XXX111')).dropDuplicates(["condition_era_id", "default_ccsr_category_op_clean"]).select(*keep_vars)

    #Some duplicates still exist because concept_ids matched with multiple ICDs that were in different CCSR groupings
    #Take the broader icd10cm_clean (codes of shorter length are more general). Create column to count the length of ICD code
    df = df.withColumn('icd_length', F.length("icd10cm_clean"))

    #Sort by length of icd code and choose the row with shortest icd code length (first row). If multiple rows in a condition era are the same length, take the first.
    w2 = Window.partitionBy("condition_era_id").orderBy(F.col("icd_length"))
    return df.withColumn("row",F.row_number().over(w2)) \
    .filter(F.col("row") == 1).drop("row")


#-----------------------------
# Extract Person Data
#-----------------------------

def person_all(person_train, person, Long_COVID_Silver_Standard_train, Long_COVID_Silver_Standard_Blinded):
    #Join test/train for persons
    person_train_test = person_train.unionByName(person, allowMissingColumns=True).dropDuplicates(['person_id'])

    #Join test/train for outcomes
    outcome_train_test = Long_COVID_Silver_Standard_train.unionByName(Long_COVID_Silver_Standard_Blinded, allowMissingColumns=True).dropDuplicates(['person_id'])

    #Joining person and outcome data
    person_outcome = person_train_test.join(outcome_train_test, outcome_train_test.person_id==person_train_test.person_id, 'inner').drop(outcome_train_test.person_id)


    #Cleaning demographics
    #Calculating age at covid dx, if they're 90 or older, set age at 90. If birthday data is missing, impute median.
    person_outcome = person_outcome.withColumn("date_of_birth", F.to_date(F.expr("make_date(year_of_birth, month_of_birth, 1)"), "yyyy-MM-dd"))
    person_outcome = person_outcome.withColumn("age_at_covid", F.round(F.datediff(F.col("covid_index"), F.col("date_of_birth"))/365.25, 0))



    #Imputing missing values for age_at_covid - using median since only 2% are missing
    imputer = Imputer(inputCols = ['age_at_covid'],
    outputCols = ["{}_imputed".format(a) for a in ['age_at_covid']]).setStrategy("median")
    person_outcome = imputer.fit(person_outcome).transform(person_outcome)

    #UDF to clean race variable
    def race_func(race_col):
        if race_col in ['Black or African American', 'Black']:
            return 'African American'
        if race_col in ['Other Race', 'Multiple races', 'More than one race', 'Hispanic', "Multiple race", 'Asian Indian']:
            return "Other"
        if (race_col in ['No information', 'Unknown racial group', 'No matching concept', 'Refuse to answer', None]):
            return 'Unknown'
        if race_col in ['Asian', 'Other Pacific Islander', 'Chinese', 'Native Hawaiian or Other Pacific Islander']:
            return 'Asian or Pacific islander'
        else:
            return race_col

    func_udf = F.udf(race_func)
    person_outcome = person_outcome.withColumn('race_cats',func_udf(person_outcome['race_concept_name']))
    #Removing spaces for dummy variable conversion
    person_outcome = person_outcome.withColumn('race_cats', F.regexp_replace('race_cats', ' ', '_'))
    #person_outcome.groupBy('race_cats').count().show(20, False) #Total count

    #Cleaning ethnicity
    person_outcome=person_outcome.withColumn("ethnicity_cats", F.when(F.col("ethnicity_concept_name").isin(['Other/Unknown', 'No matching concept', 'Unknown', 'No information', 'Other']), 'Other_unknown').otherwise(F.col("ethnicity_concept_name")))
    person_outcome = person_outcome.withColumn('ethnicity_cats', F.regexp_replace('ethnicity_cats', ' ', '_'))

    #Cleaning gender
    #If a persons gender is unknown or has no matching concept, making them male (48 patients, 0.08%)
    person_outcome=person_outcome.withColumn("gender_cats", F.when(F.col("gender_concept_name").isin(['UNKNOWN', 'No matching concept']), 'MALE').otherwise(F.col("gender_concept_name")))
    person_outcome = person_outcome.withColumn('gender_cats', F.regexp_replace('gender_cats', ' ', '_'))

    return person_outcome

#-----------------------------
# Covid Severity Metrics
#-----------------------------
#Severity based on WHO guidelines


def covid_severity(observation_train, observation, microvisits_to_macrovisits_train, microvisits_to_macrovisits, procedure_occurrence_train, concept_set_members, procedure_occurrence, person_all, condition_occurrence_train, condition_occurrence):
    condition_occurrence_training = condition_occurrence_train

    observation_train_test = observation_train.unionByName(observation, allowMissingColumns=True).dropDuplicates(['observation_id'])

    mm_train_test = microvisits_to_macrovisits_train.unionByName(microvisits_to_macrovisits, allowMissingColumns=True).dropDuplicates(['visit_occurrence_id'])

    po_train_test = procedure_occurrence_train.unionByName(procedure_occurrence, allowMissingColumns=True).dropDuplicates(['procedure_occurrence_id'])

    condition_train_test = condition_occurrence_train.unionByName(condition_occurrence, allowMissingColumns=True).dropDuplicates(['condition_occurrence_id'])

    #Ventilation - compared 2 codeset ids
    #469361388 - [ICU/MODS]IMV (v2) - contains 76 concepts - procedure, observation, condition - this defintion pulled up 6410 rows for 765 people which is way fewer than the 2nd definition although that definition has fewer concepts.
    #618758765 - N3C Mechanical Ventilation (v1) - contains 38 concepts and used for CC project - procedure, observation, condition. Using this definition bc it pulled up more instances and it was used for the cohort characterization paper.

    #Looking at 7 days before and 4 weeks after (end of study data) for IMV and ECMO
    #Pull in data for relevant time periods for each user
    df = person_all.withColumn('pre_covid_7', F.date_sub(person_all['covid_index'], 7)).alias("df")

    proc_subset = df.join(po_train_test, 'person_id', 'inner').filter(po_train_test.procedure_date >= df.pre_covid_7).drop('df.person_id')
    
    obs_subset = df.join(observation_train_test, 'person_id', 'inner').filter(observation_train_test.observation_date >= df.pre_covid_7).drop('df.person_id')

    cond_subset = df.join(condition_train_test, 'person_id', 'inner').filter(condition_train_test.condition_start_date >= df.pre_covid_7).drop('df.person_id')


    #Ventilation
    vent_concept = concept_set_members.filter(F.col("codeset_id")==618758765)

    #vent defintion pulled up 13,106 rows for 1346 people
    vent_proc = proc_subset.join(vent_concept, proc_subset.procedure_concept_id==vent_concept.concept_id,'inner')
    #vent_proc.groupBy("procedure_concept_name").count().show(20, False)
    #vent_proc.select(F.countDistinct("person_id")).show() #Distinct number of people

    #Searching observation table for ventilation
    vent_obs = obs_subset.join(vent_concept, obs_subset.observation_concept_id==vent_concept.concept_id,'inner')
    #vent_obs.groupBy("observation_concept_name").count().show(20, False)
    #vent_obs.select(F.countDistinct("person_id")).show() #Distinct number of people

    #Pulling in conditions table
    vent_cond = cond_subset.join(vent_concept, cond_subset.condition_concept_id==vent_concept.concept_id,'inner')
    #vent_cond.groupBy("condition_concept_name").count().show(20, False)
    #vent_cond.select(F.countDistinct("person_id")).show() #Distinct number of people

    final_vent = vent_proc.drop('data_partner_id', 'provider_id').unionByName(vent_obs.drop('data_partner_id', 'provider_id'), allowMissingColumns=True).unionByName(vent_cond.drop('data_partner_id', 'provider_id'), allowMissingColumns=True)
    #final_vent.select(F.countDistinct("person_id")).show() #Distinct number of people
    final_vent = final_vent.dropDuplicates(['person_id'])

    #ECMO - 415149730 - procedure, observation tables
    ecmo_concept = concept_set_members.filter(F.col("codeset_id")==415149730)

    #ecmo defintion pulled up xx rows for xx people
    ecmo_proc = proc_subset.join(ecmo_concept, proc_subset.procedure_concept_id==ecmo_concept.concept_id,'inner')
    #ecmo_proc.groupBy("procedure_concept_name").count().show(20, False)
    #ecmo_proc.select(F.countDistinct("person_id")).show() #Distinct number of people

    #Pulling in observation table to see if adds anyone new
    ecmo_obs = obs_subset.join(ecmo_concept, obs_subset.observation_concept_id==ecmo_concept.concept_id,'inner')
    #ecmo_obs.groupBy("observation_concept_name").count().show(20, False)
    #ecmo_obs.select(F.countDistinct("person_id")).show() #Distinct number of people

    final_ecmo = ecmo_proc.drop('data_partner_id', 'provider_id').unionByName(ecmo_obs.drop('data_partner_id', 'provider_id'), allowMissingColumns=True)
    #final_ecmo.select(F.countDistinct("person_id")).show() #Distinct number of people
    final_ecmo = final_ecmo.dropDuplicates(['person_id'])

    #Creating indicators for users that had either ECMO or IMV
    final_ecmo_vent = final_ecmo.unionByName(final_vent, allowMissingColumns=True).dropDuplicates(['person_id']).withColumn('who_severity', F.lit('Severe')).select('person_id', 'who_severity')

    #ecmo_vent_person_ids = final_ecmo_vent.rdd.map(lambda x: x.person_id).collect() #Converting column to list

    mm_train_test = mm_train_test.join(final_ecmo_vent, on="person_id", how="left").drop(final_ecmo_vent.person_id)

    #Logic - if they have a micro/macrovisit that started on or after 7 days before dx test and till the end of FU then mark them as that category
    visit_subset = df.join(mm_train_test, 'person_id', 'inner').filter(col("who_severity").isNull() & ((mm_train_test.visit_start_date >= df.pre_covid_7)|(df.covid_index.between(mm_train_test.visit_start_date, mm_train_test.visit_end_date))|(df.covid_index.between(mm_train_test.macrovisit_start_date, mm_train_test.macrovisit_end_date))))

    #Take the most severe visit per person during 7 days pre covid diagnosis to the end of follow up
    visit_grouped = visit_subset.groupby("person_id").agg(F.concat_ws(", ", F.collect_list(visit_subset.visit_concept_name)).alias("concat_visit_concept_name"))
    visit_grouped = visit_grouped.withColumn("who_severity", F.when(visit_grouped.concat_visit_concept_name.contains("Inpatient"), "Moderate").otherwise('Mild'))

    covid_severity = final_ecmo_vent.unionByName(visit_grouped, allowMissingColumns=True)
    #Add the users back in that did not have any visits during study (marking them as mild)
    no_visit = person_all.join(covid_severity, 'person_id', 'left_anti')
    print(no_visit.count())
    return covid_severity.unionByName(no_visit, allowMissingColumns=True).fillna('Mild', subset=['who_severity']).select('person_id', 'who_severity')

#-----------------------------
# Medication & Vax
#-----------------------------
#Defining Covid vaccinations and medication use

def medications_vaccinations(drug_era_train, drug_era, concept_set_members, person_all):
    drug_train_test = drug_era_train.unionByName(drug_era, allowMissingColumns=True).dropDuplicates(['drug_era_id'])

    #Creates indicator medication variable and prints frequency and number of distinct people using each drug
    def create_med_var(codeset_num, codeset_str, df):
        codeset = concept_set_members.filter(F.col("codeset_id")==codeset_num)
        concepts=codeset.rdd.map(lambda x: x.concept_id).collect() #Converting column to list
        df = df.withColumn(codeset_str, F.when((F.col("drug_concept_id").isin(concepts)), 1).otherwise(0))
        #df.groupBy(codeset_str).count().show() #Total count
        #df.filter(df[codeset_str]==1).select(F.countDistinct("person_id")).show() #Distinct number of people
        return df

    #Paxlovid - 280 drug_eras for paxlovid and 277 unique users. Also looked at codeset_ids: 798981734, 854747727, all produced the same numbers.
    drug_train_test = create_med_var(339287218, 'med_paxlovid', drug_train_test)

    #Remdesivir - 4785 drug_eras and 3236 unique users
    drug_train_test = create_med_var(96369749, 'med_remdesivir', drug_train_test)

    #Bebtelovimab (monoclonal antibodies). 231 drug_eras and 154 unique users
    drug_train_test = create_med_var(487953007, 'med_bebtelovimab', drug_train_test)

    #Baricitinib - 911 drug_eras and 594 unique users. Also looked at codeset_id: 394764748 and it produced the same numbers
    drug_train_test = create_med_var(38048732, 'med_baricitinib', drug_train_test)

    #Molnupiravir - 0 drug_eras and 0 unique users
    drug_train_test = create_med_var(643666235, 'med_molnupiravir', drug_train_test)

    #Covid Vaccinations - 11,466 drug_eras for 7741 unique_users. Also looked at codeset_id: 600531961 but it produced slightly fewer vaccinations. This definition is broader and includes the following drugs and their drug_concept_ids. Researched all of these drugs in Athena and they are valid.
    # SARS-CoV-2 (COVID-19) vaccine, mRNA-BNT162b2 0.1 MG/ML Injectable Suspension [Comirnaty] (1759206)
    # SARS-CoV-2 (COVID-19) vaccine, mRNA-1273 0.1 MG/ML Injectable Suspension (779414)
    # SARS-CoV-2 (COVID-19) vaccine, mRNA-BNT162b2 0.05 MG/ML Injectable Suspension (702118)
    # SARS-CoV-2 (COVID-19) vaccine, mRNA-1273 0.2 MG/ML Injectable Suspension [Spikevax] (779679)

    drug_train_test = create_med_var(697105949, 'covid_vaccine', drug_train_test)

    #Making pre/post groups for Covid vaccine
    drug_train_test = drug_train_test.join(person_all.select('person_id', 'covid_index'), 'person_id', 'left')

    drug_train_test = drug_train_test.withColumn('pre_covid_vaccine', F.when((F.col("covid_vaccine")==1) & (F.col("drug_era_start_date")< F.col("covid_index")), 1).otherwise(0))
    drug_train_test = drug_train_test.withColumn('post_covid_vaccine', F.when((F.col("covid_vaccine")==1) & (F.col("drug_era_start_date")>= F.col("covid_index")), 1).otherwise(0))

    drug_sums = drug_train_test.groupBy("person_id").agg(F.sum("med_paxlovid").alias("med_paxlovid_sum"),\
    F.sum("pre_covid_vaccine").alias("pre_covid_vaccine_sum"),\
    F.sum("post_covid_vaccine").alias("post_covid_vaccine_sum"),\
    F.sum("med_remdesivir").alias("med_remdesivir_sum"),\
    F.sum("med_bebtelovimab").alias("med_bebtelovimab_sum"),\
    F.sum("med_baricitinib").alias("med_baricitinib_sum"),\
    F.sum("med_molnupiravir").alias("med_molnupiravir_sum"))

    #Creating 1 variable for meds as indicator since drug use relatively infrequent
    drug_sums = drug_sums.withColumn('med_sum', sum(drug_sums[col] for col in ['med_remdesivir_sum', 'med_bebtelovimab_sum', 'med_baricitinib_sum', 'med_molnupiravir_sum', 'med_paxlovid_sum']))
    drug_sums = drug_sums.withColumn('med_sum', F.when(F.col("med_sum")>0, 1).otherwise(0))
    return drug_sums.drop('med_remdesivir_sum', 'med_bebtelovimab_sum', 'med_baricitinib_sum', 'med_molnupiravir_sum', 'med_paxlovid_sum')

#-----------------------------
# Assessing CCI Count
#-----------------------------

#Calculating the sum of Charleson Comorbidity Index components for each patient
#CCI codeset from appendix here: https://static-content.springer.com/esm/art%3A10.1186%2Fs12879-022-07776-7/MediaObjects/12879_2022_7776_MOESM1_ESM.pdf

def cci_count(icd_match, concept_set_members, person_all):
    #Using pre-existing CCI concept sets
    cci_cats = [535274723, 359043664, 78746470, 719585646, 403438288, 73549360, 494981955, 248333963, 378462283, 259495957, 489555336, 510748896, 514953976, 376881697, 220495690, 7650044049, 652711186]
    cci = concept_set_members.filter(F.col("codeset_id").isin(cci_cats))
    cci_person = icd_match.join(cci, icd_match.condition_concept_id==cci.concept_id,'left') #Matching conditions to CCI concepts

    #Counting number of CCI components for each person
    cci = cci_person.filter(cci_person.concept_set_name.isNotNull()).dropDuplicates(['person_id', 'concept_set_name']).groupBy('person_id').count().withColumnRenamed("count", "cci_count")

    #Grabbing Covid index date to create pre/post variables
    icd_outcome = icd_match.join(person_all.select('person_id', 'covid_index'), 'person_id', 'left')

    #Adding time component (pre/post) to CCSR categories
    icd_outcome = icd_outcome.withColumn("pre_post_covid", F.when((F.col("condition_era_start_date") < F.col("covid_index")), "pre").otherwise("post"))
    icd_outcome = icd_outcome.withColumn("pre_post_condition", F.concat(icd_outcome.pre_post_covid, F.lit('_'), icd_outcome.default_ccsr_category_op_clean))

    #Joining CCI count column with CCSR data
    return icd_outcome.join(cci, 'person_id', 'left').fillna(0, subset=['cci_count'])


#-----------------------------
# Pivot
#-----------------------------

#Pivoting and then counting the occurrence of each pre/post condition
def pivot_by_person(cci_count):
    df = cci_count.groupBy("person_id").pivot("pre_post_condition").agg(F.lit(1)).fillna(0)

    df_sum = df.select(df.columns[1:]) #Grabbing condition columns
    cols = [F.sum(F.col(x)).alias(x) for x in df_sum.columns]
    agg_df = df_sum.agg(*cols).toPandas()
    return pd.melt(agg_df, var_name='condition', value_name='condition_count')

#-----------------------------
# Conditions Removal/ Clean
#-----------------------------
#Removing conditions that occur fewer than 1000 times in dataset (if <1000 people have a condition it occurs in fewer than 1.7% of the study population)

start_time = time.time()

def conditions_only(pivot_by_person, cci_count):
    sub1000 = pivot_by_person.filter(pivot_by_person.condition_count<1000)
    sub1000_removed = cci_count.join(sub1000, cci_count.pre_post_condition==sub1000.condition, 'left_anti') #Keeping rows with conditions occurring 1000+ times
    conds = sub1000_removed.groupBy("person_id").pivot("pre_post_condition").agg(F.lit(1)).fillna(0)
    #Rejoining with CCI counts
    df = conds.join(cci_count.dropDuplicates(['person_id', 'cci_count']).select('person_id', 'cci_count'), ['person_id'], 'right')
    print(f"Execution time: {time.time() - start_time}")
    return df

#-----------------------------
# Cohort creation
#-----------------------------

#Merging all datasets into cohort
def cohort(conditions_only, person_all, medications_vaccinations, covid_severity):
    person = person_all.select('person_id', 'pasc_code_after_four_weeks', 'pasc_code_prior_four_weeks', 'age_at_covid_imputed', 'gender_cats', 'race_cats', 'ethnicity_cats')
    merged = person.join(conditions_only, 'person_id', 'left').join(medications_vaccinations, 'person_id', 'left').join(covid_severity, 'person_id', 'left')
    return merged.fillna(0, subset=(merged.columns[7:-1])) #Filling NAs for the condition columns

#-----------------------------
# Model prep
#-----------------------------
#Removing users diagnosed with PASC in the 4 weeks after diagnosis and creating dummy variables for categorical variables

def model_prep(cohort):
    df = cohort.loc[(cohort['pasc_code_prior_four_weeks']==0)|(pd.isna(cohort['pasc_code_after_four_weeks']))]
    df = df.drop(columns=['pasc_code_prior_four_weeks', 'race_cats', 'ethnicity_cats'])
    return pd.get_dummies(df, columns=['who_severity', 'gender_cats'], drop_first=True)


#-----------------------------
# Model hyperparams
#-----------------------------

start_time = time.time()

def xgb_hyperparam_tuning(model_prep):

    training_data = model_prep[~pd.isna(model_prep['pasc_code_after_four_weeks'])]

    X = training_data.drop(columns=['pasc_code_after_four_weeks', 'person_id'])
    Y = training_data['pasc_code_after_four_weeks']
    x_train, x_test, y_train, y_test = train_test_split(X, Y, test_size=0.2, random_state=123, stratify=Y)

    space={'max_depth': hp.quniform("max_depth", 3, 18, 1),
        'gamma': hp.uniform ('gamma', 1,9),
        'reg_alpha' : hp.quniform('reg_alpha', 40,180,1),
        'reg_lambda' : hp.uniform('reg_lambda', 0,1),
        'colsample_bytree' : hp.uniform('colsample_bytree', 0.5,1),
        'min_child_weight' : hp.quniform('min_child_weight', 0, 10, 1),
        'scale_pos_weight' : hp.choice('scale_pos_weight', [4, 5, 6, 7, 8]),
        'n_estimators': 180,
        'seed': 0
    }

    def objective(space):
        clf=XGBClassifier(n_estimators =space['n_estimators'], max_depth = int(space['max_depth']), gamma = space['gamma'],
                        reg_alpha = int(space['reg_alpha']),min_child_weight=int(space['min_child_weight']),
                        colsample_bytree=int(space['colsample_bytree']), scale_pos_weight=space['scale_pos_weight'], use_label_encoder=False)

        evaluation = [(x_train, y_train), (x_test, y_test)]

        clf.fit(x_train, y_train,
                eval_set=evaluation, eval_metric="auc",
                early_stopping_rounds=10,verbose=False)

        predictions = clf.predict(x_test)

        accuracy = accuracy_score(y_test, predictions)
        print("Accuracy: %.2f%%" % (accuracy * 100.0))
        f1 = f1_score(y_test, predictions)
        print("f1: %.2f%%" % (f1 * 100.0))
        precision = precision_score(y_test, predictions)
        print("Precision: %.2f%%" % (precision * 100.0))
        recall = recall_score(y_test, predictions)
        print("Recall: %.2f%%" % (recall * 100.0))
        roc_auc = roc_auc_score(y_test, predictions)
        print("roc_auc: %.2f%%" % (roc_auc * 100.0))
        cm = confusion_matrix(y_test, predictions)
        print('tn', cm[0, 0], 'fp', cm[0, 1], 'fn', cm[1, 0], 'tp', cm[1, 1])

        return {'loss': 1-roc_auc, 'status': STATUS_OK }

    trials = Trials()

    best_hyperparams = fmin(fn = objective,
                            space = space,
                            algo = tpe.suggest,
                            max_evals = 100,
                            trials = trials)

    print("The best hyperparameters are : ","\n")
    print(best_hyperparams)

    best_dict = {key: [val] for key, val in best_hyperparams.items()}

    print(f"Execution time: {time.time() - start_time}")
    return pd.DataFrame.from_dict(best_dict)

#-----------------------------
# Final Predictions
#-----------------------------

#Running class-weighted XGBoost classifier on cohort using optimal parameters from hyperparameter tuning.

start_time = time.time()

def ruvos_predictions(xgb_hyperparam_tuning, model_prep, person):

    #Separating training from test sets
    train = pd.DataFrame(model_prep.loc[~model_prep['person_id'].isin(person['person_id'].tolist())])
    test = model_prep.loc[model_prep['person_id'].isin(person['person_id'].tolist())]

    x_train = train.drop(columns=['pasc_code_after_four_weeks', 'person_id'])
    x_test = test.drop(columns=['pasc_code_after_four_weeks', 'person_id'])

    y_train = train['pasc_code_after_four_weeks']
    y_test = test['pasc_code_after_four_weeks']

    # X = remove_sub1000.drop(columns=['pasc_code_after_four_weeks', 'person_id'])
    # Y = remove_sub1000['pasc_code_after_four_weeks']
    # x_train, x_test, y_train, y_test = train_test_split(X, Y, test_size=0.2, random_state=123, stratify=Y)

    #Using best parameters from hyperparameter tuning
    params = xgb_hyperparam_tuning.to_dict(orient="list")
    params = {k:v[0] for (k,v) in params.items()}
    params_int = {k:int(v) for (k,v) in params.items() if any(k in x for x in ['max_depth', 'min_child_weight', 'reg_alpha', 'scale_pos_weight'])}
    params['use_label_encoder'] = False
    params.update(params_int)

    #Fit model - Class Weighted XGBoost
    model = XGBClassifier(**params)
    model.fit(x_train, y_train)

    print(model)

    #Predictions on test set
    y_prob = model.predict_proba(x_test)
    # keep probabilities for the positive outcome only
    print (y_prob)
    y_prob = y_prob[:, 1]

    predictions = [round(value) for value in y_prob]

    #Running evaluation metrics
    """accuracy = accuracy_score(y_test, predictions)
    print("Accuracy: %.2f%%" % (accuracy * 100.0))
    f1 = f1_score(y_test, predictions)
    print("f1: %.2f%%" % (f1 * 100.0))
    precision = precision_score(y_test, predictions)
    print("Precision: %.2f%%" % (precision * 100.0))
    recall = recall_score(y_test, predictions)
    print("Recall: %.2f%%" % (recall * 100.0))
    roc_auc = roc_auc_score(y_test, predictions)
    print("roc_auc: %.2f%%" % (roc_auc * 100.0))
    cm = confusion_matrix(y_test, predictions)
    print('tn', cm[0, 0], 'fp', cm[0, 1], 'fn', cm[1, 0], 'tp', cm[1, 1])"""

# Plot feature importance - top 10
    plt.style.use('default')
    #Gain
    plot_importance(model, max_num_features=30, importance_type="gain", grid=False, xlabel="Average gain", show_values=False)
    plt.tight_layout()
    plt.show()

    #Weight
    plot_importance(model, max_num_features=30, grid=False, xlabel="Weight", show_values=False)
    plt.tight_layout()
    plt.show()

    #Return the person_id for test set
    test_indices = x_test.index.tolist()
    person_id_df = model_prep.iloc[test_indices]['person_id'].to_frame().reset_index(drop=True)

    #Returns dataframe with person_id and predicted value.
    output_with_preds = pd.concat([pd.Series(y_prob).to_frame(name='predictions'), person_id_df], axis=1)
    print(f"Execution time: {time.time() - start_time}")
    return output_with_preds



spark = SparkSession.builder.getOrCreate()

#==============================================================================
# Load Datasets
#==============================================================================

#concept_set_members
concept_set_members = spark.read.option("header",True).csv("../data/concept_set_members.csv")

#concepts and concept relationships
concept_relationship = spark.read.option("header",True).csv("../data/concept_relationship.csv")
concept = spark.read.option("header",True).csv("../data/concept.csv")

concept_names = concept.select("concept_id", "concept_name")

#DXCCSR_v2021
DXCCSR_v2021 = spark.read.option("header",True).csv("../data/DXCCSR_v2021.csv")

#condition_era
condition_era_train = spark.read.option("header",True).csv("../data/training/condition_era.csv")
condition_era_train = condition_era_train.join(concept_names,concept_names.concept_id == condition_era_train.condition_concept_id,'left')\
    .select('condition_era_id', 'person_id', 'condition_concept_id', 'concept_name', 'condition_era_start_date', 'condition_era_end_date', 'condition_occurrence_count')\
        .withColumnRenamed('concept_name', 'condition_concept_name')


condition_era = spark.read.option("header",True).csv("../data/testing/condition_era.csv")
condition_era = condition_era.join(concept_names,concept_names.concept_id == condition_era.condition_concept_id,'left')\
    .select('condition_era_id', 'person_id', 'condition_concept_id', 'concept_name', 'condition_era_start_date', 'condition_era_end_date', 'condition_occurrence_count')\
        .withColumnRenamed('concept_name', 'condition_concept_name')


#drug_era
drug_era_train = spark.read.option("header",True).csv("../data/training/drug_era.csv")
drug_era_train = drug_era_train.join(concept_names,concept_names.concept_id == drug_era_train.drug_concept_id,'left')\
    .select('drug_era_id', 'person_id', 'drug_concept_id', 'concept_name', 'drug_era_start_date', 'drug_era_end_date', 'drug_exposure_count', 'gap_days')\
        .withColumnRenamed('concept_name', 'drug_concept_name')

drug_era = spark.read.option("header",True).csv("../data/testing/drug_era.csv")
drug_era = drug_era.join(concept_names,concept_names.concept_id == drug_era.drug_concept_id,'left')\
    .select('drug_era_id', 'person_id', 'drug_concept_id', 'concept_name', 'drug_era_start_date', 'drug_era_end_date', 'drug_exposure_count', 'gap_days')\
        .withColumnRenamed('concept_name', 'drug_concept_name')


#observation
observation_train = spark.read.option("header",True).csv("../data/training/observation.csv").alias('observation_train')
observation_train = observation_train.join(concept_names, concept_names.concept_id == observation_train.observation_concept_id, 'left')\
    .select('observation_train.*', 'concept_name').withColumnRenamed('concept_name', 'observation_concept_name')

observation = spark.read.option("header",True).csv("../data/testing/observation.csv").alias('observation')
observation = observation.join(concept_names, concept_names.concept_id == observation.observation_concept_id, 'left')\
    .select('observation.*', 'concept_name').withColumnRenamed('concept_name', 'observation_concept_name')


#microvisit to macrovisit tables
microvisits_to_macrovisits_train = spark.read.option("header",True).csv("../data/training/microvisits_to_macrovisits.csv").alias('microvisits_to_macrovisits_train')
microvisits_to_macrovisits_train = microvisits_to_macrovisits_train.join(concept_names, concept_names.concept_id == microvisits_to_macrovisits_train.visit_concept_id, 'left')\
    .select('microvisits_to_macrovisits_train.*', 'concept_name').withColumnRenamed('concept_name', 'visit_concept_name')

microvisits_to_macrovisits = spark.read.option("header",True).csv("../data/testing/microvisits_to_macrovisits.csv").alias('microvisits_to_macrovisits')
microvisits_to_macrovisits = microvisits_to_macrovisits.join(concept_names, concept_names.concept_id == microvisits_to_macrovisits.visit_concept_id, 'left')\
    .select('microvisits_to_macrovisits.*', 'concept_name').withColumnRenamed('concept_name', 'visit_concept_name')


#procedure occurrence
procedure_occurrence_train = spark.read.option("header",True).csv("../data/training/procedure_occurrence.csv").alias('procedure_occurrence_train')
procedure_occurrence_train = procedure_occurrence_train.join(concept_names, concept_names.concept_id == procedure_occurrence_train.procedure_concept_id, 'left')\
    .select('procedure_occurrence_train.*', 'concept_name').withColumnRenamed('concept_name', 'procedure_concept_name')

procedure_occurrence = spark.read.option("header",True).csv("../data/testing/procedure_occurrence.csv").alias('procedure_occurrence')
procedure_occurrence = procedure_occurrence.join(concept_names, concept_names.concept_id == procedure_occurrence.procedure_concept_id, 'left')\
    .select('procedure_occurrence.*', 'concept_name').withColumnRenamed('concept_name', 'procedure_concept_name')


#condition occurrence
condition_occurrence_train = spark.read.option("header",True).csv("../data/training/condition_occurrence.csv").alias('condition_occurrence_train')
condition_occurrence_train = condition_occurrence_train.join(concept_names, concept_names.concept_id == condition_occurrence_train.condition_concept_id, 'left')\
    .select('condition_occurrence_train.*', 'concept_name').withColumnRenamed('concept_name', 'condition_concept_name')

condition_occurrence = spark.read.option("header",True).csv("../data/testing/condition_occurrence.csv").alias('condition_occurrence')
condition_occurrence = condition_occurrence.join(concept_names, concept_names.concept_id == condition_occurrence.condition_concept_id, 'left')\
    .select('condition_occurrence.*', 'concept_name').withColumnRenamed('concept_name', 'condition_concept_name')



#long_covid_silver_standard
Long_COVID_Silver_Standard_train = spark.read.option("header",True).csv("../data/training/long_covid_silver_standard.csv")
Long_COVID_Silver_Standard_Blinded = spark.read.option("header",True).csv("../data/testing/long_covid_silver_standard.csv")

#person
person_train = spark.read.option("header",True).csv("../data/training/person.csv")

person_train = person_train.join(concept_names,concept_names.concept_id == person_train.race_concept_id,'left')\
    .select('year_of_birth', 'gender_source_value', 'ethnicity_concept_id', 'provider_id', \
            'race_source_concept_id', 'person_id', 'person_source_value', 'month_of_birth', \
            'gender_source_concept_id', 'ethnicity_source_concept_id', 'care_site_id', \
            'day_of_birth', 'ethnicity_source_value', 'location_id', 'race_concept_id', 'concept_name',\
            'gender_concept_id', 'birth_datetime', 'race_source_value')\
        .withColumnRenamed('concept_name', 'race_concept_name')

person_train = person_train.join(concept_names,concept_names.concept_id == person_train.gender_concept_id,'left')\
    .select('year_of_birth', 'gender_source_value', 'ethnicity_concept_id', 'provider_id', \
            'race_source_concept_id', 'person_id', 'person_source_value', 'month_of_birth', \
            'gender_source_concept_id', 'ethnicity_source_concept_id', 'care_site_id', \
            'day_of_birth', 'ethnicity_source_value', 'location_id', 'race_concept_id', 'race_concept_name',\
            'gender_concept_id', 'concept_name', 'birth_datetime', 'race_source_value')\
        .withColumnRenamed('concept_name', 'gender_concept_name')

person_train = person_train.join(concept_names,concept_names.concept_id == person_train.ethnicity_concept_id,'left')\
    .select('year_of_birth', 'gender_source_value', 'ethnicity_concept_id', 'concept_name', 'provider_id', \
            'race_source_concept_id', 'person_id', 'person_source_value', 'month_of_birth', \
            'gender_source_concept_id', 'ethnicity_source_concept_id', 'care_site_id', \
            'day_of_birth', 'ethnicity_source_value', 'location_id', 'race_concept_id', 'race_concept_name',\
            'gender_concept_id', 'gender_concept_name', 'birth_datetime', 'race_source_value')\
        .withColumnRenamed('concept_name', 'ethnicity_concept_name')


person = spark.read.option("header",True).csv("../data/testing/person.csv")

person = person.join(concept_names,concept_names.concept_id == person.race_concept_id,'left')\
    .select('year_of_birth', 'gender_source_value', 'ethnicity_concept_id', 'provider_id', \
            'race_source_concept_id', 'person_id', 'person_source_value', 'month_of_birth', \
            'gender_source_concept_id', 'ethnicity_source_concept_id', 'care_site_id', \
            'day_of_birth', 'ethnicity_source_value', 'location_id', 'race_concept_id', 'concept_name',\
            'gender_concept_id', 'birth_datetime', 'race_source_value')\
        .withColumnRenamed('concept_name', 'race_concept_name')

person = person.join(concept_names,concept_names.concept_id == person.gender_concept_id,'left')\
    .select('year_of_birth', 'gender_source_value', 'ethnicity_concept_id', 'provider_id', \
            'race_source_concept_id', 'person_id', 'person_source_value', 'month_of_birth', \
            'gender_source_concept_id', 'ethnicity_source_concept_id', 'care_site_id', \
            'day_of_birth', 'ethnicity_source_value', 'location_id', 'race_concept_id', 'race_concept_name',\
            'gender_concept_id', 'concept_name', 'birth_datetime', 'race_source_value')\
        .withColumnRenamed('concept_name', 'gender_concept_name')

person = person.join(concept_names,concept_names.concept_id == person.ethnicity_concept_id,'left')\
    .select('year_of_birth', 'gender_source_value', 'ethnicity_concept_id', 'concept_name', 'provider_id', \
            'race_source_concept_id', 'person_id', 'person_source_value', 'month_of_birth', \
            'gender_source_concept_id', 'ethnicity_source_concept_id', 'care_site_id', \
            'day_of_birth', 'ethnicity_source_value', 'location_id', 'race_concept_id', 'race_concept_name',\
            'gender_concept_id', 'gender_concept_name', 'birth_datetime', 'race_source_value')\
        .withColumnRenamed('concept_name', 'ethnicity_concept_name')

print('INSERT_ENCLAVE_DATA')



###------------------------------------------------------------------------###
### Manually import remaining datasets below
###------------------------------------------------------------------------###

## Train & test versions of:
# 1. concept                       --> concept_train & concept
# 2. condition_occurence           --> condition_occurence_train & condition_occurence
# 3. microvisits_to_macrovisits    --> microvisits_to_macrovisits_train & microvisits_to_macrovisits
# 4. observation                   --> observation_train & observation
# 5. procedure_occurence           --> procedure_occurence_train & procedure_occurence

###------------------------------------------------------------------------###






#==============================================================================
# Pipeline Script
#==============================================================================

#-----------------------------
# Creat Mapped Concepts
#-----------------------------
mapped_concepts_ = mapped_concepts(concept_relationship, concept, DXCCSR_v2021)

#-----------------------------
# Map Conditions
#-----------------------------
#Joining conditions to mapped concepts. This is a many to many match, icd_match code cleans it to make 1:1 mappings.
condition_mapped_ = condition_mapped(mapped_concepts_, condition_era_train, condition_era)


#-----------------------------
# ICD Matching
#-----------------------------
icd_match_= icd_match(condition_mapped_)


#-----------------------------
# Extract Person Data
#-----------------------------
person_all_ = person_all(person_train, person, Long_COVID_Silver_Standard_train, Long_COVID_Silver_Standard_Blinded)


#-----------------------------
# Covid Severity Metrics
#-----------------------------
#Severity based on WHO guidelines

covid_severity_ = covid_severity(observation_train, observation, microvisits_to_macrovisits_train, microvisits_to_macrovisits, procedure_occurrence_train, concept_set_members, procedure_occurrence, person_all_, condition_occurrence_train, condition_occurrence)

#-----------------------------
# Medication & Vax
#-----------------------------
#Defining Covid vaccinations and medication use

medications_vaccinations_ = medications_vaccinations(drug_era_train, drug_era, concept_set_members, person_all_)


#-----------------------------
# Assessing CCI Count
#-----------------------------

#Calculating the sum of Charleson Comorbidity Index components for each patient
#CCI codeset from appendix here: https://static-content.springer.com/esm/art%3A10.1186%2Fs12879-022-07776-7/MediaObjects/12879_2022_7776_MOESM1_ESM.pdf
cci_count_ = cci_count(icd_match_, concept_set_members, person_all_)


#-----------------------------
# Pivot
#-----------------------------

#Pivoting and then counting the occurrence of each pre/post condition
pivot_by_person_ = pivot_by_person(cci_count_)

#-----------------------------
# Conditions Removal/ Clean
#-----------------------------
#Removing conditions that occur fewer than 1000 times in dataset (if <1000 people have a condition it occurs in fewer than 1.7% of the study population)

start_time = time.time()
conditions_only_ = conditions_only(pivot_by_person_, cci_count_)



#-----------------------------
# Cohort creation
#-----------------------------

#Merging all datasets into cohort
cohort_ = cohort(conditions_only_, person_all_, medications_vaccinations_, covid_severity_)

#-----------------------------
# Model prep
#-----------------------------
#Removing users diagnosed with PASC in the 4 weeks after diagnosis and creating dummy variables for categorical variables
model_prep_ = model_prep(cohort_)


#-----------------------------
# Model hyperparams
#-----------------------------

start_time = time.time()
xgb_hyperparam_tuning_ = xgb_hyperparam_tuning(model_prep_)

#-----------------------------
# Final Predictions
#-----------------------------

#Running class-weighted XGBoost classifier on cohort using optimal parameters from hyperparameter tuning.

start_time = time.time()
final_preds = ruvos_predictions(xgb_hyperparam_tuning_, model_prep_, person)



## End
