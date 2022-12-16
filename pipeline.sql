

@transform_pandas(
    Output(rid="ri.vector.main.execute.f1ee2db4-b8a7-4d4a-8b31-351198d76285"),
    condition_mapped=Input(rid="ri.foundry.main.dataset.a1fd31d0-a0ba-4cd0-b3e4-20033a743646")
)
SELECT *
FROM condition_mapped

