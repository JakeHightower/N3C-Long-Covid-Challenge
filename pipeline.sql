

@transform_pandas(
    Output(rid="ri.vector.main.execute.84b3b764-d157-4cd0-8a8c-2ca55dc67e76"),
    duplicate_icds=Input(rid="ri.foundry.main.dataset.9879cf19-e3bf-496d-91a8-9fd05140bde6")
)
SELECT count(default_ccsr_category_op_clean) as ccsr_count, default_ccsr_category_op_clean
FROM duplicate_icds
group by default_ccsr_category_op_clean
order by ccsr_count desc

