

@transform_pandas(
    Output(rid="ri.vector.main.execute.afad3330-c701-4b0b-9ee1-0922868e898b"),
    microvisits_to_macrovisits_train=Input(rid="ri.foundry.main.dataset.d77a701f-34df-48a1-a71c-b28112a07ffa")
)
SELECT count(distinct visit_occurrence_id)
FROM microvisits_to_macrovisits_train

