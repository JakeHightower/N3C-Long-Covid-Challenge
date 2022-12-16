

@transform_pandas(
    Output(rid="ri.vector.main.execute.a827f870-9bda-4dfb-a9b2-484f8119d3ec"),
    Long_COVID_Silver_Standard_train=Input(rid="ri.foundry.main.dataset.3ea1038c-e278-4b0e-8300-db37d3505671")
)
SELECT count(distinct person_id)
FROM Long_COVID_Silver_Standard_train

