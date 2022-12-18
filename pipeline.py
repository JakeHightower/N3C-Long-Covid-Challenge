

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d5a82b65-cb77-4c5b-a6f9-1d2c24b34a9b"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    icd_match=Input(rid="ri.foundry.main.dataset.8ad54572-0a0e-48bc-b56f-2d3c006b57b6"),
    person_all=Input(rid="ri.foundry.main.dataset.0d5a4646-5221-432c-b937-8b8841f6162d")
)
#Calculating the sum of Charleson Comorbidity Index components for each patient
#CCI codeset from appendix here: https://static-content.springer.com/esm/art%3A10.1186%2Fs12879-022-07776-7/MediaObjects/12879_2022_7776_MOESM1_ESM.pdf

from pyspark.sql import functions as F

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

    

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fb2bf844-e144-4ce9-b1dc-864ed7d22eaf"),
    conditions_only=Input(rid="ri.foundry.main.dataset.cb07b6ff-9f7a-4fbe-9769-bdbbc922fc9d"),
    covid_severity=Input(rid="ri.foundry.main.dataset.0791f5da-f1fb-47d0-bc39-b910d43e0b73"),
    medications_vaccinations=Input(rid="ri.foundry.main.dataset.55dcfe75-eed3-4017-9d13-84e227bd509f"),
    person_all=Input(rid="ri.foundry.main.dataset.0d5a4646-5221-432c-b937-8b8841f6162d")
)
#Merging all datasets into cohort
def cohort(conditions_only, person_all, medications_vaccinations, covid_severity):
    columns_to_drop = ['location_id', 'person_data_partner_id', 'gender_concept_id', 'race_concept_id', 'ethnicity_concept_id', 'bmi', 'state', 'bmi_cat', 'race_concept_name', 'gender_concept_name', 'ethnicity_concept_name']
    person = person_all.select('person_id', 'test_ind', 'pasc_code_after_four_weeks', 'pasc_code_prior_four_weeks', 'age_at_covid_imputed', 'gender_cats', 'race_cats', 'ethnicity_cats')
    merged = person.join(conditions_only, 'person_id', 'left').join(medications_vaccinations, 'person_id', 'left').join(covid_severity, 'person_id', 'left')
    return merged.fillna(0, subset=(merged.columns[8:-1])) #Filling NAs for the condition columns 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a1fd31d0-a0ba-4cd0-b3e4-20033a743646"),
    condition_era=Input(rid="ri.foundry.main.dataset.52d99538-b41d-4cb8-887a-9ac775c829f3"),
    condition_era_train=Input(rid="ri.foundry.main.dataset.e9ff83ed-a71c-4abe-a0e2-c204e624cd8c"),
    mapped_concepts=Input(rid="ri.foundry.main.dataset.313bf22e-6ba2-46a6-be7b-742db516104c")
)
#Joining conditions to mapped concepts. This is a many to many match, icd_match code cleans it to make 1:1 mappings. 
def condition_mapped(mapped_concepts, condition_era_train, condition_era):
    #38,044 patients have conditions in the condition_era_train table
    condition_train_test = condition_era_train.unionByName(condition_era, allowMissingColumns=True).dropDuplicates(['condition_era_id'])
    return condition_train_test.join(mapped_concepts, condition_train_test.condition_concept_id==mapped_concepts.concept_id_2,'left')

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.cb07b6ff-9f7a-4fbe-9769-bdbbc922fc9d"),
    cci_count=Input(rid="ri.foundry.main.dataset.d5a82b65-cb77-4c5b-a6f9-1d2c24b34a9b"),
    pivot_by_person=Input(rid="ri.foundry.main.dataset.92ab38b0-054c-49d8-8473-8606f00dd020")
)
#Removing conditions that occur fewer than 100 times in dataset
from pyspark.sql import functions as F

def conditions_only(pivot_by_person, cci_count):
    sub100 = pivot_by_person.filter(pivot_by_person.condition_count<100) 
    sub100_removed = cci_count.join(sub100, cci_count.pre_post_condition==sub100.condition, 'left_anti') #Keeping rows with conditions occurring 100+ times
    conds = sub100_removed.groupBy("person_id").pivot("pre_post_condition").agg(F.lit(1)).fillna(0)
    #Rejoining with CCI counts
    return conds.join(cci_count.dropDuplicates(['person_id', 'cci_count']).select('person_id', 'cci_count'), ['person_id'], 'right')
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0791f5da-f1fb-47d0-bc39-b910d43e0b73"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.3e01546f-f110-4c67-a6db-9063d2939a74"),
    condition_occurrence_train=Input(rid="ri.foundry.main.dataset.2f496793-6a4e-4bf4-b0fc-596b277fb7e2"),
    microvisits_to_macrovisits=Input(rid="ri.foundry.main.dataset.f5008fa4-e736-4244-88e1-1da7a68efcdb"),
    microvisits_to_macrovisits_train=Input(rid="ri.foundry.main.dataset.d77a701f-34df-48a1-a71c-b28112a07ffa"),
    observation=Input(rid="ri.foundry.main.dataset.fc1ce22e-9cf6-4335-8ca7-aa8c733d506d"),
    observation_train=Input(rid="ri.foundry.main.dataset.f9d8b08e-3c9f-4292-b603-f1bfa4336516"),
    person_all=Input(rid="ri.foundry.main.dataset.0d5a4646-5221-432c-b937-8b8841f6162d"),
    procedure_occurrence=Input(rid="ri.foundry.main.dataset.88523aaa-75c3-4b55-a79a-ebe27e40ba4f"),
    procedure_occurrence_train=Input(rid="ri.foundry.main.dataset.9a13eb06-de7d-482b-8f91-fb8c144269e3")
)
from pyspark.sql import functions as F

#Severity based on WHO guidelines
def covid_severity(observation_train, observation, microvisits_to_macrovisits_train, microvisits_to_macrovisits, procedure_occurrence_train, concept_set_members, procedure_occurrence, person_all, condition_occurrence_train, condition_occurrence):
    condition_occurrence_training = condition_occurrence_train
    
    observation_train_test = observation_train.unionByName(observation, allowMissingColumns=True)
    observation_train_test = observation_train_test.dropDuplicates(['observation_id']) 

    mm_train_test = microvisits_to_macrovisits_train.unionByName(microvisits_to_macrovisits, allowMissingColumns=True)
    mm_train_test = mm_train_test.dropDuplicates(['visit_occurrence_id']) 

    po_train_test = procedure_occurrence_train.unionByName(procedure_occurrence, allowMissingColumns=True)
    po_train_test = po_train_test.dropDuplicates(['procedure_occurrence_id']) 

    condition_train_test = condition_occurrence_train.unionByName(condition_occurrence, allowMissingColumns=True)
    condition_train_test = condition_train_test.dropDuplicates(['condition_occurrence_id']) 
    
    #Ventilation - compared 2 codeset ids
    #469361388 - [ICU/MODS]IMV (v2) - contains 76 concepts - procedure, observation, condition - this defintion pulled up 6410 rows for 765 people which is way fewer than the 2nd definition although that definition has fewer concepts. 
    #618758765 - N3C Mechanical Ventilation (v1) - contains 38 concepts and used for CC project - procedure, observation, condition. Using this definition bc it pulled up more instances and it was used for the cohort characterization paper. 

    #Looking at 7 days before and 4 weeks after (end of study data) for IMV and ECMO
    #Pull in data for relevant time periods for each user
    df = person_all.withColumn('pre_covid_7', F.date_sub(person_all['covid_index'], 7))
    
    proc_subset = df.join(po_train_test, 'person_id', 'inner').filter(po_train_test.procedure_date >= df.pre_covid_7)
    obs_subset = df.join(observation_train_test, 'person_id', 'inner').filter(observation_train_test.observation_date >= df.pre_covid_7)
    cond_subset = df.join(condition_train_test, 'person_id', 'inner').filter(condition_train_test.condition_start_date >= df.pre_covid_7)
    
    #Ventilation
    vent_concept = concept_set_members.filter(F.col("codeset_id")==618758765)
    
    #vent defintion pulled up 13,106 rows for 1346 people 
    vent_proc = proc_subset.join(vent_concept, proc_subset.procedure_concept_id==vent_concept.concept_id,'inner')
    vent_proc.groupBy("procedure_concept_name").count().show(20, False)
    vent_proc.select(F.countDistinct("person_id")).show() #Distinct number of people

    #Searching observation table for ventilation
    vent_obs = obs_subset.join(vent_concept, obs_subset.observation_concept_id==vent_concept.concept_id,'inner')
    vent_obs.groupBy("observation_concept_name").count().show(20, False)
    vent_obs.select(F.countDistinct("person_id")).show() #Distinct number of people
 
    #Pulling in conditions table
    vent_cond = cond_subset.join(vent_concept, cond_subset.condition_concept_id==vent_concept.concept_id,'inner')
    vent_cond.groupBy("condition_concept_name").count().show(20, False)
    vent_cond.select(F.countDistinct("person_id")).show() #Distinct number of people

    final_vent = vent_proc.drop('data_partner_id', 'provider_id').unionByName(vent_obs.drop('data_partner_id', 'provider_id'), allowMissingColumns=True).unionByName(vent_cond.drop('data_partner_id', 'provider_id'), allowMissingColumns=True)
    final_vent.select(F.countDistinct("person_id")).show() #Distinct number of people
    final_vent = final_vent.dropDuplicates(['person_id'])

    #ECMO - 415149730 - procedure, observation tables
    ecmo_concept = concept_set_members.filter(F.col("codeset_id")==415149730)
    
    #ecmo defintion pulled up xx rows for xx people 
    ecmo_proc = proc_subset.join(ecmo_concept, proc_subset.procedure_concept_id==ecmo_concept.concept_id,'inner')
    ecmo_proc.groupBy("procedure_concept_name").count().show(20, False)
    ecmo_proc.select(F.countDistinct("person_id")).show() #Distinct number of people

    #Pulling in observation table to see if adds anyone new
    ecmo_obs = obs_subset.join(ecmo_concept, obs_subset.observation_concept_id==ecmo_concept.concept_id,'inner')
    ecmo_obs.groupBy("observation_concept_name").count().show(20, False)
    ecmo_obs.select(F.countDistinct("person_id")).show() #Distinct number of people
 
    final_ecmo = ecmo_proc.drop('data_partner_id', 'provider_id').unionByName(ecmo_obs.drop('data_partner_id', 'provider_id'), allowMissingColumns=True)
    final_ecmo.select(F.countDistinct("person_id")).show() #Distinct number of people
    final_ecmo = final_ecmo.dropDuplicates(['person_id'])
    
    #Creating indicators for users that had either ECMO or IMV
    final_ecmo_vent = final_ecmo.unionByName(final_vent, allowMissingColumns=True).dropDuplicates(['person_id']).withColumn('who_severity', F.lit('Severe')).select('person_id', 'who_severity') 
    
    ecmo_vent_person_ids = final_ecmo_vent.rdd.map(lambda x: x.person_id).collect() #Converting column to list

    #Logic - if they have a micro/macrovisit that started on or after 7 days before dx test and till the end of FU then mark them as that category
    visit_subset = df.join(mm_train_test, 'person_id', 'inner').filter((~mm_train_test.person_id.isin(ecmo_vent_person_ids)) & ((mm_train_test.visit_start_date >= df.pre_covid_7)|(df.covid_index.between(mm_train_test.visit_start_date, mm_train_test.visit_end_date))|(df.covid_index.between(mm_train_test.macrovisit_start_date, mm_train_test.macrovisit_end_date))))
    
    #Take the most severe visit per person during 7 days pre covid diagnosis to the end of follow up
    visit_grouped = visit_subset.groupby("person_id").agg(F.concat_ws(", ", F.collect_list(visit_subset.visit_concept_name)).alias("concat_visit_concept_name"))
    visit_grouped = visit_grouped.withColumn("who_severity", F.when(visit_grouped.concat_visit_concept_name.contains("Inpatient"), "Moderate").otherwise('Mild'))

    covid_severity = final_ecmo_vent.unionByName(visit_grouped, allowMissingColumns=True)
    #Add the users back in that did not have any visits during study (marking them as mild)
    no_visit = person_all.join(covid_severity, 'person_id', 'left_anti')
    print(no_visit.count())
    return covid_severity.unionByName(no_visit, allowMissingColumns=True).fillna('Mild', subset=['who_severity']).select('person_id', 'who_severity')

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8ad54572-0a0e-48bc-b56f-2d3c006b57b6"),
    condition_mapped=Input(rid="ri.foundry.main.dataset.a1fd31d0-a0ba-4cd0-b3e4-20033a743646")
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.313bf22e-6ba2-46a6-be7b-742db516104c"),
    DXCCSR_v2021_2=Input(rid="ri.foundry.main.dataset.5a6e7797-98f7-4ab8-b4e9-8c8fe4de6d4c"),
    concept=Input(rid="ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772"),
    concept_relationship=Input(rid="ri.foundry.main.dataset.0469a283-692e-4654-bb2e-26922aff9d71")
)
from pyspark.sql import functions as F

def mapped_concepts(concept_relationship, concept, DXCCSR_v2021_2):
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
    ccsr = DXCCSR_v2021_2.withColumn("icd10cm_clean", F.regexp_replace("`ICD-10-CM_CODE`", "'", ''))
    ccsr = ccsr.withColumn("default_ccsr_category_op_clean", F.regexp_replace("Default_CCSR_CATEGORY_OP", "'", ''))
    
    #Final table has 125,203 rows. 26,723 rows (containing 20,724 unique icd10 codes) are in the concept table but not in the ccsr table
    return concept_map.join(ccsr, concept_map.concept_code_clean==ccsr.icd10cm_clean,'outer')

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.55dcfe75-eed3-4017-9d13-84e227bd509f"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    drug_era=Input(rid="ri.foundry.main.dataset.5d7b2d96-8549-4207-823f-f4e95be34ed3"),
    drug_era_train=Input(rid="ri.foundry.main.dataset.9f7a8197-ea20-48ef-8350-2dfb4f964750"),
    person_all=Input(rid="ri.foundry.main.dataset.0d5a4646-5221-432c-b937-8b8841f6162d")
)
#Defining Covid vaccinations and medication use
from pyspark.sql import functions as F

def medications_vaccinations(drug_era_train, drug_era, concept_set_members, person_all):
    drug_train_test = drug_era_train.unionByName(drug_era, allowMissingColumns=True).dropDuplicates(['drug_era_id'])
    
    #Creates indicator medication variable and prints frequency and number of distinct people using each drug
    def create_med_var(codeset_num, codeset_str, df):
        codeset = concept_set_members.filter(F.col("codeset_id")==codeset_num) 
        concepts=codeset.rdd.map(lambda x: x.concept_id).collect() #Converting column to list  
        df = df.withColumn(codeset_str, F.when((F.col("drug_concept_id").isin(concepts)), 1).otherwise(0))
        df.groupBy(codeset_str).count().show() #Total count
        df.filter(df[codeset_str]==1).select(F.countDistinct("person_id")).show() #Distinct number of people
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

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7e421db4-19fe-437d-b705-f696bbc9f831"),
    cohort=Input(rid="ri.foundry.main.dataset.fb2bf844-e144-4ce9-b1dc-864ed7d22eaf")
)
#Removing users diagnosed with PASC in the 4 weeks after diagnosis and creating dummy variables for categorical variables
import pandas as pd

def model_prep(cohort):
    df = cohort.loc[cohort['pasc_code_prior_four_weeks']==0] 
    df = df.drop(columns=['test_ind', 'pasc_code_prior_four_weeks', 'race_cats', 'ethnicity_cats'])
    return pd.get_dummies(df, columns=['who_severity', 'gender_cats'], drop_first=True)
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b908cc43-e8f6-4ad5-8211-03bcb179f7d4"),
    model_prep=Input(rid="ri.foundry.main.dataset.7e421db4-19fe-437d-b705-f696bbc9f831")
)
#Data preparation for neural network. Scale continuous columns and merge data back with one hot encoded columns

from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
import time

def normalize(model_prep):
    start_time = time.time()
    # Select only cols that need scaling (e.g. skip OHE)
    df = model_prep.select("person_id", "age_at_covid_imputed", "med_sum", "cci_count", "pre_covid_vaccine_sum", "post_covid_vaccine_sum",)
    print( "Before Scaling :")
    df.show(5)

    # UDF for converting column type from vector to double type
    unlist = udf(lambda x: round(float(list(x)[0]),3), DoubleType())

    # Iterating over columns to be scaled
    for i in df.columns:
        # skip if person_id
        if i == "person_id":
            continue
        # VectorAssembler Transformation - Converting column to vector type
        assembler = VectorAssembler(inputCols=[i],outputCol=i+"_Vect")

        # MinMaxScaler Transformation
        scaler = MinMaxScaler(inputCol=i+"_Vect", outputCol=i+"_Scaled")

        # Pipeline of VectorAssembler and MinMaxScaler
        pipeline = Pipeline(stages=[assembler, scaler])

        # Fitting pipeline on dataframe
        df = pipeline.fit(df).transform(df).withColumn(i+"_Scaled", unlist(i+"_Scaled")).drop(i, i+"_Vect")

    print("After Scaling :")
    df.show(5)

    # Join scaled df back to original
    joined_df = model_prep.join(df,["person_id"])

    joined_df.show(5)

    print(f"Execution time: {time.time() - start_time}")
    return joined_df.drop("age_at_covid_imputed", "med_sum", "cci_count", "pre_covid_vaccine_sum", "post_covid_vaccine_sum",)
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0d5a4646-5221-432c-b937-8b8841f6162d"),
    Long_COVID_Silver_Standard_Blinded=Input(rid="ri.foundry.main.dataset.cb65632b-bdff-4aa9-8696-91bc6667e2ba"),
    Long_COVID_Silver_Standard_train=Input(rid="ri.foundry.main.dataset.3ea1038c-e278-4b0e-8300-db37d3505671"),
    person=Input(rid="ri.foundry.main.dataset.06629068-25fc-4802-9b31-ead4ed515da4"),
    person_train=Input(rid="ri.foundry.main.dataset.f71ffe18-6969-4a24-b81c-0e06a1ae9316")
)
from pyspark.sql import functions as F
from pyspark.ml.feature import Imputer

def person_all(person_train, person, Long_COVID_Silver_Standard_train, Long_COVID_Silver_Standard_Blinded):
    #Join test/train for persons
    person_test_ind = person.withColumn('test_ind', F.lit(1)) #Adding indicator that this person is part of the test set
    person_train_test = person_train.unionByName(person_test_ind, allowMissingColumns=True).fillna(0, subset='test_ind')#.dropDuplicates(['person_id']) #UNCOMMENT FOR FINAL
    person_train_test = person_train_test.filter(person_train_test.test_ind==0) #DELETE FOR FINAL

    #Join test/train for outcomes
    outcome_train_test = Long_COVID_Silver_Standard_train.unionByName(Long_COVID_Silver_Standard_Blinded, allowMissingColumns=True)#.dropDuplicates(['person_id']) #UNCOMMENT FOR FINAL
    outcome_train_test = outcome_train_test.filter(outcome_train_test.pasc_code_after_four_weeks.isNotNull()) #DELETE FOR FINAL
    
    #Joining person and outcome data
    person_outcome = person_train_test.join(outcome_train_test, 'person_id', 'inner')
    
    #Cleaning demographics
    #Calculating age at covid dx, if they're 90 or older, set age at 90. If birthday data is missing, impute median.
    person_outcome = person_outcome.withColumn("date_of_birth", F.to_date(F.expr("make_date(year_of_birth, month_of_birth, 1)"), "yyyy-MM-dd"))
    person_outcome = person_outcome.withColumn("age_at_covid", F.when(F.col("is_age_90_or_older"), 90).otherwise(F.round(F.datediff(F.col("covid_index"), F.col("date_of_birth"))/365.25, 0)))

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
    person_outcome.groupBy('race_cats').count().show(20, False) #Total count

    #Cleaning ethnicity
    person_outcome=person_outcome.withColumn("ethnicity_cats", F.when(F.col("ethnicity_concept_name").isin(['Other/Unknown', 'No matching concept', 'Unknown', 'No information', 'Other']), 'Other_unknown').otherwise(F.col("ethnicity_concept_name"))) 
    person_outcome = person_outcome.withColumn('ethnicity_cats', F.regexp_replace('ethnicity_cats', ' ', '_'))

    #Cleaning gender
    #If a persons gender is unknown or has no matching concept, making them male (48 patients, 0.08%)
    person_outcome=person_outcome.withColumn("gender_cats", F.when(F.col("gender_concept_name").isin(['UNKNOWN', 'No matching concept']), 'MALE').otherwise(F.col("gender_concept_name"))) 
    person_outcome = person_outcome.withColumn('gender_cats', F.regexp_replace('gender_cats', ' ', '_'))

    return person_outcome

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.92ab38b0-054c-49d8-8473-8606f00dd020"),
    cci_count=Input(rid="ri.foundry.main.dataset.d5a82b65-cb77-4c5b-a6f9-1d2c24b34a9b")
)
from pyspark.sql import functions as F
import pandas as pd

#Pivoting and then counting the occurrence of each pre/post condition 
def pivot_by_person(cci_count):
    df = cci_count.groupBy("person_id").pivot("pre_post_condition").agg(F.lit(1)).fillna(0)

    df_sum = df.select(df.columns[1:]) #Grabbing condition columns
    cols = [F.sum(F.col(x)).alias(x) for x in df_sum.columns]
    agg_df = df_sum.agg(*cols).toPandas()
    return pd.melt(agg_df, var_name='condition', value_name='condition_count')

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6edb8486-6f1c-4af3-b85a-f3b0dff380c1"),
    model_prep=Input(rid="ri.foundry.main.dataset.7e421db4-19fe-437d-b705-f696bbc9f831"),
    pivot_by_person=Input(rid="ri.foundry.main.dataset.92ab38b0-054c-49d8-8473-8606f00dd020")
)
#Doing this just for XGBoost model
def remove_sub1000(model_prep, pivot_by_person):
    sub1000 = pivot_by_person.filter((pivot_by_person.condition_count>100)&(pivot_by_person.condition_count<1000)) 
    #Create list of variables to drop in modeling dataset
    cols_to_drop=sub1000.rdd.map(lambda x: x.condition).collect() #Converting column to list  
    return model_prep.drop(*cols_to_drop)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f267bdc4-9cee-45e7-8ba2-1042dbaa3623"),
    remove_sub1000=Input(rid="ri.foundry.main.dataset.6edb8486-6f1c-4af3-b85a-f3b0dff380c1"),
    xgb_hyperparam_tuning=Input(rid="ri.foundry.main.dataset.cec62123-8cbd-42a8-8f20-cd5a18438cff")
)
#Running class-weighted XGBoost classifier on cohort using optimal parameters from hyperparameter tuning. 

from xgboost import XGBClassifier, plot_importance
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score, confusion_matrix, roc_curve
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
import numpy as np 
import pandas as pd
import time
start_time = time.time()

def ruvos_predictions(xgb_hyperparam_tuning, remove_sub1000):

    X = remove_sub1000.drop(columns=['pasc_code_after_four_weeks', 'person_id'])
    Y = remove_sub1000['pasc_code_after_four_weeks']
    x_train, x_test, y_train, y_test = train_test_split(X, Y, test_size=0.2, random_state=123, stratify=Y)

    #Using best parameters from hyperparameter tuning
    params = xgb_hyperparam_tuning.to_dict(orient="list")
    params = {k:v[0] for (k,v) in params.items()}
    params = {k:int(v) for (k,v) in params.items() if any(k in x for x in ['max_depth', 'min_child_weight', 'reg_alpha', 'scale_pos_weight']) else k:v}
    params['use_label_encoder'] = False

    print(params)

    
    #Fit model - Class Weighted XGBoost
    model = XGBClassifier(**params) 
    model.fit(x_train, y_train) 

    print(model)

    #Predictions on test set
    y_prob = model.predict_proba(x_test)
    # keep probabilities for the positive outcome only
    y_prob = y_prob[:, 1]
    print(y_prob)

    y_pred = model.predict(x_test)
    print(y_pred)
    predictions = [round(value) for value in y_pred]

    #Running evaluation metrics
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

# Plot feature importance - top 10
    plot_importance(model, max_num_features=10)
    plt.tight_layout()
    plt.show()

    #Return the person_id for test set
    test_indices = x_test.index.tolist()
    person_id_df = remove_sub1000.iloc[test_indices]['person_id'].to_frame().reset_index(drop=True)

    #Returns dataframe with person_id, pasc_code_after_four_weeks and predicted value to use for ensembling. 
    output_with_preds = pd.concat([pd.Series(y_prob).to_frame(name='predictions'), y_test.to_frame(name='pasc_code_after_four_weeks').reset_index(drop=True), person_id_df], axis=1) #x_test.reset_index(drop=True),
    print(f"Execution time: {time.time() - start_time}")
    return output_with_preds
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.cec62123-8cbd-42a8-8f20-cd5a18438cff"),
    remove_sub1000=Input(rid="ri.foundry.main.dataset.6edb8486-6f1c-4af3-b85a-f3b0dff380c1")
)
import pandas as pd
from hyperopt import STATUS_OK, Trials, fmin, hp, tpe
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score, confusion_matrix, roc_curve
from sklearn.model_selection import train_test_split
import time
start_time = time.time()

def xgb_hyperparam_tuning(remove_sub1000):
    
    X = remove_sub1000.drop(columns=['pasc_code_after_four_weeks', 'person_id'])
    Y = remove_sub1000['pasc_code_after_four_weeks']
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

