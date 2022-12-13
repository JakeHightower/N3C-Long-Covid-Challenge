

@transform_pandas(
    Output(rid="ri.vector.main.execute.2d0dc8c6-248b-41c3-b06f-be288eea5683"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    icd_match=Input(rid="ri.vector.main.execute.96ee7feb-6634-42e5-b423-7c2baa1e0855")
)
#There is a general CCI codeset_id that matches to the measurement table. This indicates that the measurement is from CCI, not using for now (codeset_id=2351618)
#CCI codeset from appendix here: https://static-content.springer.com/esm/art%3A10.1186%2Fs12879-022-07776-7/MediaObjects/12879_2022_7776_MOESM1_ESM.pdf
#Findings from paper: moderate to severe liver disease, renal disease, metastatic solid tumor, and MI were the top four fatal comorbidities among patients who were hospitalized for COVID-19... Consistently, our study demonstrated that both men and women presented increased inflammation and coagulation, as suggested by the higher levels of CRP, ferritin, procalcitonin, NT proBNP and lymphopenia were at a higher risk of death.

from pyspark.sql import functions as F

def cci_join(icd_match, concept_set_members):
    cci_cats = [535274723, 359043664, 78746470, 719585646, 403438288, 73549360, 494981955, 248333963, 378462283, 259495957, 489555336, 510748896, 514953976, 376881697, 220495690, 7650044049, 652711186] 
    cci = concept_set_members.filter(F.col("codeset_id").isin(cci_cats))
    cci_person = icd_match.join(cci, icd_match.condition_concept_id==cci.concept_id,'left')

    cci = cci_person.filter(cci_person.concept_set_name.isNotNull()).dropDuplicates(['person_id', 'concept_set_name']).groupBy('person_id').count().withColumnRenamed("count", "cci_count")    
    
    #Adding time component (pre/post) to CCSR categories
    icd_match = icd_match.withColumn("pre_post_covid", F.when((F.col("condition_era_start_date") < F.col("covid_index")), "pre").otherwise("post")) 
    icd_match = icd_match.withColumn("pre_post_condition", F.concat(icd_match.pre_post_covid, F.lit('_'), icd_match.default_ccsr_category_op_clean)) 

    #Rejoining with CCSR count column with icd data
    cci_ccsr = icd_match.join(cci, 'person_id', 'left').fillna(0, subset=['cci_count'])

    # cci_person.filter((cci_person.concept_set_name.isNotNull()) & (cci_person.person_id==5019396812315161384)).dropDuplicates(['concept_set_name']).show() # This person has 9 CCIs 
    #Removing spaces and dashes 
    # cci_person = cci_person.withColumn("concept_set_name_edited", F.regexp_replace("concept_set_name", " ", ''))
    # cci_person = cci_person.withColumn("concept_set_name_edited", F.regexp_replace("concept_set_name_edited", "-", '_'))  
    
    # #Date difference between condition and covid - same as N3C comborbidities paper
    # cci_person = cci_person.withColumn("condition_to_covid", F.datediff(F.col("condition_era_start_date"), F.col("covid_index")))

    return cci_ccsr

    

    

@transform_pandas(
    Output(rid="ri.vector.main.execute.135459d9-ff11-45b7-be37-2e2552c4a89a"),
    cci_join=Input(rid="ri.vector.main.execute.2d0dc8c6-248b-41c3-b06f-be288eea5683"),
    pivot_by_person=Input(rid="ri.vector.main.execute.ceecf240-b697-4628-94de-8ac0dba046ca")
)
from pyspark.sql import functions as F

def conditions_only(pivot_by_person, cci_join):
    sub100 = pivot_by_person.filter(pivot_by_person.condition_count<100)
    sub100_removed = cci_join.join(sub100, cci_join.pre_post_condition==sub100.condition, 'left_anti')
    conds = sub100_removed.groupBy("person_id").pivot("pre_post_condition").agg(F.lit(1)).fillna(0)
    #Rejoining with CCI counts
    return conds.join(cci_join.dropDuplicates(['person_id', 'cci_count']).select('person_id', 'cci_count'), ['person_id'], 'right')
    

@transform_pandas(
    Output(rid="ri.vector.main.execute.9983da15-9e7d-4d73-9ae3-da74847b93fc"),
    duplicate_icds=Input(rid="ri.vector.main.execute.bf34a516-47c9-44b2-a70f-421f24eb5d64")
)
#condition_era_id = 1000050713616695994, person_id=8904989188366942906

def duplicate_example(duplicate_icds):
    return duplicate_icds.filter(duplicate_icds.condition_era_id==1000050713616695994)    

@transform_pandas(
    Output(rid="ri.vector.main.execute.bf34a516-47c9-44b2-a70f-421f24eb5d64"),
    icd_match=Input(rid="ri.vector.main.execute.96ee7feb-6634-42e5-b423-7c2baa1e0855")
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def duplicate_icds(icd_match):
    #All duplicates
    duplicate_icds = icd_match.groupBy("condition_era_id").count().filter("count > 1").join(icd_match, 'condition_era_id', 'left').sort(F.desc("count"))
    
    # duplicate_icds.select(F.countDistinct("person_id")).show() #26,158 - Distinct number of people with duplicates
    duplicate_icds.select(F.countDistinct("condition_era_id")).show() # - Distinct condition eras with duplicates

    #Take the icd10cm_clean thats shorter since that's likely the broader category. Create column to count the length of ICD code
    duplicate_icds = duplicate_icds.withColumn('icd_length', F.length("icd10cm_clean")) 
    
    #List of all condition_era_ids that contain a PRG
    # prg_eras=duplicate_icds.filter(duplicate_icds.default_ccsr_category_op_clean=='PRG').rdd.map(lambda x: x.condition_era_id).collect()  
    # prg = duplicate_icds.filter(duplicate_icds.condition_era_id.isin(prg_eras)) #Condition eras that contain a pregnancy code
    # no_prg = duplicate_icds.filter(~duplicate_icds.condition_era_id.isin(prg_eras)) #Condition eras that don't contain a pregnancy code
    
    #Sort by length of icd code and then choose the row with shortest icd code length (first row). If multiple rows in a condition era are the same length, this takes the first
    w2 = Window.partitionBy("condition_era_id").orderBy(F.col("icd_length"))
    one_row_pp = duplicate_icds.withColumn("row",F.row_number().over(w2)) \
    .filter(F.col("row") == 1).drop("row")

    return one_row_pp
    # prg.select(F.countDistinct("condition_era_id")).show() #Distinct condition eras containing pregnancy
    # no_prg_1row.select(F.countDistinct("condition_era_id")).show() #Distinct condition eras that did not contain pregnancies 
    # no_prg_1row.show(30)
    # return prg

    # return no_prg.groupBy('condition_era_id').agg(F.min('icd_length').alias('B'))
    
    

    
    
    #Note: people can only have 1 condition_concept_id per condition_era_id. If they sought care in the same time period for multiple conditions then those are recorded in separate condition_era_ids.
    
    # print(icd_dups.select(F.countDistinct("person_id")).show()) #29,401 person_ids have multiple icd groups
    # return icd_dups.sort("condition_era_id")
    # icd_dups_legit = icd_dups.filter(icd_dups.default_ccsr_category_op_clean!='XXX111').sort("condition_era_id") #Removing ccsr category that is not legit
    
    # test = icd_dups_legit.exceptAll(icd_dups_legit.dropDuplicates(['condition_era_id'])

    # print(test.select(F.countDistinct("person_id")).show())  #24,551 have duplicates now
    # return test
    

@transform_pandas(
    Output(rid="ri.vector.main.execute.cf3f475a-ff06-4a9c-8c4e-6c86669794c9")
)
#No longer using this code but pieces of it are in other cells. Can erase eventually.

#If there are multiple rows per condition_era, taking the first one. Doing this temporarily until find some other solution.
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, when, concat, lit

def first_row_pp(): 
    final_deduplicate_cssrs = cci_join
    w2 = Window.partitionBy("condition_era_id").orderBy(col("default_ccsr_category_op_clean"))
    df = final_deduplicate_cssrs.withColumn("row",row_number().over(w2)) \
    .filter(col("row") == 1).drop("row")
    return df.withColumn("pre_post_condition", concat(df.pre_post_covid, lit('_'), df.default_ccsr_category_op_clean))

@transform_pandas(
    Output(rid="ri.vector.main.execute.1408546e-c5b8-4119-b8d1-b41fc5d7cb9e"),
    no_icd_match=Input(rid="ri.vector.main.execute.3ddc4744-e58d-4835-aa93-38c3c898b1df")
)
from pyspark.sql import functions as F

def frequency_all(no_icd_match):
    return no_icd_match.groupBy('condition_concept_name', 'condition_concept_id').count().sort(F.desc("count"))

@transform_pandas(
    Output(rid="ri.vector.main.execute.0d7687bf-d562-40e5-81e2-1b4fa90fffd7"),
    no_icd_match=Input(rid="ri.vector.main.execute.3ddc4744-e58d-4835-aa93-38c3c898b1df")
)
from pyspark.sql import functions as F

def frequency_long_covid(no_icd_match):
    return no_icd_match.filter(no_icd_match.pasc_code_after_four_weeks==1).groupBy('condition_concept_name', 'condition_concept_id').count().sort(F.desc("count"))

@transform_pandas(
    Output(rid="ri.vector.main.execute.96ee7feb-6634-42e5-b423-7c2baa1e0855"),
    person_mapped=Input(rid="ri.vector.main.execute.80e302b7-3a7b-491a-882a-12086f468c6f")
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def icd_match(person_mapped):
    keep_vars = ["person_id", "condition_era_id", "condition_era_start_date", "Default_CCSR_CATEGORY_DESCRIPTION_OP", "icd10cm_clean", "default_ccsr_category_op_clean", "condition_concept_id", "condition_concept_name", "covid_index", "pasc_code_after_four_weeks", "pasc_code_prior_four_weeks", "time_to_pasc"]

    #Recoding all pregnancy categories into 1 group since they are highly correlated and adding some manual mappings for concepts that did not map directly to ICD10 codes

    person_mapped = person_mapped.withColumn('default_ccsr_category_op_clean', F.when(F.col("default_ccsr_category_op_clean").startswith('PRG'), 'PRG').when(F.col("condition_concept_id")==4113821, 'MBD005').when(F.col("condition_concept_id")==4195384, 'SYM013').otherwise(F.col("default_ccsr_category_op_clean")))

    df = person_mapped.filter((person_mapped.default_ccsr_category_op_clean.isNotNull()) & (person_mapped.default_ccsr_category_op_clean!='XXX111')).dropDuplicates(["condition_era_id", "default_ccsr_category_op_clean"]).select(*keep_vars)

    #There are still some duplicates because concept_ids matched with multiple ICDs 
    #Take the icd10cm_clean thats shorter since that's likely the broader category. Create column to count the length of ICD code
    df = df.withColumn('icd_length', F.length("icd10cm_clean"))
    
    #Sort by length of icd code and then choose the row with shortest icd code length (first row). If multiple rows in a condition era are the same length, this takes whichever is first
    w2 = Window.partitionBy("condition_era_id").orderBy(F.col("icd_length"))
    return df.withColumn("row",F.row_number().over(w2)) \
    .filter(F.col("row") == 1).drop("row")

@transform_pandas(
    Output(rid="ri.vector.main.execute.9180ee4f-3b1a-46f2-8bdb-be5b381abef9"),
    no_icd_match=Input(rid="ri.vector.main.execute.3ddc4744-e58d-4835-aa93-38c3c898b1df")
)
def manually_coded_conds(no_icd_match):
    #Adding 2 manually coded conditions back into the mix
    

@transform_pandas(
    Output(rid="ri.vector.main.execute.296f5a26-0ada-4c63-aa3f-f194f6c1f22c"),
    DXCCSR_v2021_2=Input(rid="ri.foundry.main.dataset.5a6e7797-98f7-4ab8-b4e9-8c8fe4de6d4c"),
    concept=Input(rid="ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772"),
    concept_relationship=Input(rid="ri.foundry.main.dataset.0469a283-692e-4654-bb2e-26922aff9d71")
)
from pyspark.sql import functions as F

def mapped_concepts(concept_relationship, concept, DXCCSR_v2021_2):
    maps_to = concept_relationship.filter((concept_relationship.relationship_id=="Maps to") & (concept_relationship.concept_id_1!=concept_relationship.concept_id_2))
    
    concept_icd10 = concept.filter((concept.domain_id=="Condition") & (concept.vocabulary_id=="ICD10CM") & (concept.invalid_reason.isNull()))
    concept_icd10 = concept_icd10.withColumn("concept_code_clean", F.regexp_replace("concept_code", "\.", ''))   
    
    concept_icd10 = concept_icd10.alias('concept_icd10')
    maps_to = maps_to.alias('maps_to')

    #Joining concept_relationship and concept tables. This table has 120,696 rows.
    concept_map = maps_to.join(concept_icd10,concept_icd10.concept_id == maps_to.concept_id_1,'inner').select('concept_icd10.concept_id', 'concept_icd10.concept_name', 'concept_icd10.concept_code_clean', 'maps_to.concept_id_1', 'maps_to.concept_id_2')
    
    #OG DXCCSR_v2021_2 data table has 73,211 rows
    ccsr = DXCCSR_v2021_2.withColumn("icd10cm_clean", F.regexp_replace("`ICD-10-CM_CODE`", "'", ''))
    ccsr = ccsr.withColumn("default_ccsr_category_op_clean", F.regexp_replace("Default_CCSR_CATEGORY_OP", "'", ''))
    
    #Final table has 125,203 rows. 26,723 rows (containing 20,724 unique icd10 codes) are in the concept table but not in the ccsr table
    return concept_map.join(ccsr, concept_map.concept_code_clean==ccsr.icd10cm_clean,'outer')

    # return concept_icd10.groupBy('valid_end_date').count()

@transform_pandas(
    Output(rid="ri.vector.main.execute.435e8f4b-b69c-49ee-adf4-9467fb8cca6a"),
    person_all=Input(rid="ri.vector.main.execute.600acb7d-9609-46a3-9da2-cff8a471e152")
)
def no_conditions(person_all, condition_era_all):
    #19,628 people don't have any rows in condition_era table 
    return person_all.join(condition_era_all, person_all.person_id == condition_era_all.person_id, how='left_anti') 

@transform_pandas(
    Output(rid="ri.vector.main.execute.3ddc4744-e58d-4835-aa93-38c3c898b1df"),
    icd_match=Input(rid="ri.vector.main.execute.96ee7feb-6634-42e5-b423-7c2baa1e0855"),
    person_mapped=Input(rid="ri.vector.main.execute.80e302b7-3a7b-491a-882a-12086f468c6f")
)
from pyspark.sql import functions as F

def no_icd_match(person_mapped, icd_match):    
    no_match = person_mapped.filter(person_mapped.default_ccsr_category_op_clean.isNull())
    return no_match.join(icd_match, icd_match.condition_era_id == no_match.condition_era_id, how='left_anti')

    

@transform_pandas(
    Output(rid="ri.vector.main.execute.c91cb327-cf16-473b-9ec3-a8ceec0398ba"),
    no_icd_match=Input(rid="ri.vector.main.execute.3ddc4744-e58d-4835-aa93-38c3c898b1df")
)
from pyspark.sql import functions as F

def no_icd_match_duplicates(no_icd_match):
    return no_icd_match.groupBy("condition_era_id").count().filter("count > 1").join(no_icd_match, 'condition_era_id', 'left').sort(F.desc("count"))

@transform_pandas(
    Output(rid="ri.vector.main.execute.31690664-c9f6-44ac-95c3-2db4c9ef15cf"),
    mapped_concepts=Input(rid="ri.vector.main.execute.296f5a26-0ada-4c63-aa3f-f194f6c1f22c")
)
#Connected to duplicate_example box
def odd_mapping(mapped_concepts):
   return mapped_concepts.filter(mapped_concepts.concept_id_2 == 4234997)
    
    

@transform_pandas(
    Output(rid="ri.vector.main.execute.600acb7d-9609-46a3-9da2-cff8a471e152"),
    person_test_ind=Input(rid="ri.vector.main.execute.9e84c413-83e6-4f58-aa9c-6cd2a71674ff"),
    person_train=Input(rid="ri.foundry.main.dataset.f71ffe18-6969-4a24-b81c-0e06a1ae9316")
)
def person_all(person_test_ind, person_train):
    return person_train.unionByName(person_test_ind, allowMissingColumns=True).fillna(0, subset='test_ind')

@transform_pandas(
    Output(rid="ri.vector.main.execute.b301a068-50e2-4187-aea4-0314d9429ad8"),
    Long_COVID_Silver_Standard=Input(rid="ri.foundry.main.dataset.3ea1038c-e278-4b0e-8300-db37d3505671"),
    condition_era=Input(rid="ri.foundry.main.dataset.e9ff83ed-a71c-4abe-a0e2-c204e624cd8c"),
    person_all=Input(rid="ri.vector.main.execute.600acb7d-9609-46a3-9da2-cff8a471e152")
)
from pyspark.sql import functions as F

#38,044 people have conditions in the condition_era table
def person_condition(person_all, condition_era, Long_COVID_Silver_Standard):
    condition_outcome = condition_era.join(Long_COVID_Silver_Standard, 'person_id','inner')
    return condition_outcome.drop('data_partner_id').join(person_all, 'person_id', 'inner')

@transform_pandas(
    Output(rid="ri.vector.main.execute.dcdad9bf-abec-40a7-9c13-57aba4b7973c"),
    icd_match=Input(rid="ri.vector.main.execute.96ee7feb-6634-42e5-b423-7c2baa1e0855"),
    no_icd_match=Input(rid="ri.vector.main.execute.3ddc4744-e58d-4835-aa93-38c3c898b1df")
)
#Outputs users who have only condition_eras that have no match to CCSR (as opposed to some that have matches and some that don't)
 def person_count(no_icd_match, icd_match):
    no = no_icd_match.dropDuplicates(['person_id'])
    yes = icd_match.dropDuplicates(['person_id'])
    return no.join(yes, 'person_id', 'left_anti')

@transform_pandas(
    Output(rid="ri.vector.main.execute.80e302b7-3a7b-491a-882a-12086f468c6f"),
    mapped_concepts=Input(rid="ri.vector.main.execute.296f5a26-0ada-4c63-aa3f-f194f6c1f22c"),
    person_condition=Input(rid="ri.vector.main.execute.b301a068-50e2-4187-aea4-0314d9429ad8")
)
def person_mapped(person_condition, mapped_concepts):
    # ltd_df = person_condition.limit(200)
    return person_condition.join(mapped_concepts, person_condition.condition_concept_id==mapped_concepts.concept_id_2,'left')

@transform_pandas(
    Output(rid="ri.vector.main.execute.9e84c413-83e6-4f58-aa9c-6cd2a71674ff"),
    person_test=Input(rid="ri.foundry.main.dataset.06629068-25fc-4802-9b31-ead4ed515da4")
)
from pyspark.sql import functions as F

def person_test_ind(person_test):
    #Adding indicator that this person is part of the test set
    return person_test.withColumn('test_ind', F.lit(1))

@transform_pandas(
    Output(rid="ri.vector.main.execute.ceecf240-b697-4628-94de-8ac0dba046ca"),
    cci_join=Input(rid="ri.vector.main.execute.2d0dc8c6-248b-41c3-b06f-be288eea5683")
)
#pivot data example pulled from: /UNITE/N3C Training Area/Practice Area - Public and Example Data/Machine learning examples/synthea_RF/conditions_and_demographics_synthea_dialysis_RF/RandomForest_Dialysis_Synthea

from pyspark.sql import functions as F
import pandas as pd

#Pivoting and then counting the occurrence of each pre/post condition 
def pivot_by_person(cci_join):
    df = cci_join.groupBy("person_id").pivot("pre_post_condition").agg(F.lit(1)).fillna(0)

    df_sum = df.select(df.columns[1:]) #Grabbing condition columns
    cols = [F.sum(F.col(x)).alias(x) for x in df_sum.columns]
    agg_df = df_sum.agg(*cols).toPandas()
    return pd.melt(agg_df, var_name='condition', value_name='condition_count')
    
    
    
    # df_cci = first_row_pp.filter(first_row_pp.pre_post_cci_condition.isNotNull()).groupBy("person_id").pivot("pre_post_cci_condition").agg(F.lit(1)).fillna(0) 
    # return df_ccsr.join(df_cci, ["person_id"], 'left').fillna(0)

@transform_pandas(
    Output(rid="ri.vector.main.execute.b816b156-d14a-4ab4-9478-ce6de6e18baf"),
    icd_match=Input(rid="ri.vector.main.execute.96ee7feb-6634-42e5-b423-7c2baa1e0855")
)
from pyspark.sql import functions as F

def unacceptable_diagnoses(icd_match):
    return icd_match.filter(icd_match.default_ccsr_category_op_clean=='XXX111').groupBy('icd10cm_clean').count().sort(F.desc("count"))

