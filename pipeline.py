

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
    Output(rid="ri.vector.main.execute.600acb7d-9609-46a3-9da2-cff8a471e152"),
    person_test_ind=Input(rid="ri.vector.main.execute.9e84c413-83e6-4f58-aa9c-6cd2a71674ff"),
    person_train=Input(rid="ri.foundry.main.dataset.f71ffe18-6969-4a24-b81c-0e06a1ae9316")
)
def person_all(person_test_ind, person_train):
    return person_train.unionByName(person_test_ind, allowMissingColumns=True).fillna(0, subset='test_ind')

@transform_pandas(
    Output(rid="ri.vector.main.execute.9e84c413-83e6-4f58-aa9c-6cd2a71674ff"),
    person_test=Input(rid="ri.foundry.main.dataset.06629068-25fc-4802-9b31-ead4ed515da4")
)
from pyspark.sql import functions as F

def person_test_ind(person_test):
    #Adding indicator that this person is part of the test set
    return person_test.withColumn('test_ind', F.lit(1))

