

@transform_pandas(
    Output(rid="ri.vector.main.execute.afad3330-c701-4b0b-9ee1-0922868e898b"),
    microvisits_to_macrovisits_train=Input(rid="ri.foundry.main.dataset.d77a701f-34df-48a1-a71c-b28112a07ffa")
)
SELECT count(distinct visit_occurrence_id)
FROM microvisits_to_macrovisits_train

@transform_pandas(
    Output(rid="ri.vector.main.execute.38a3b53b-0371-4456-a5e6-b0d9f6506e02"),
    DXCCSR_v2021_2=Input(rid="ri.foundry.main.dataset.5a6e7797-98f7-4ab8-b4e9-8c8fe4de6d4c")
)
SELECT count(distinct Default_CCSR_CATEGORY_DESCRIPTION_OP)
FROM DXCCSR_v2021_2

@transform_pandas(
    Output(rid="ri.vector.main.execute.ba29d7b1-bb25-4c44-9338-5b585c649ae8"),
    person=Input(rid="ri.foundry.main.dataset.06629068-25fc-4802-9b31-ead4ed515da4")
)
SELECT count(gender_concept_name), gender_concept_name
FROM person
group by gender_concept_name

