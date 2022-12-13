

@transform_pandas(
    Output(rid="ri.vector.main.execute.84b3b764-d157-4cd0-8a8c-2ca55dc67e76"),
    duplicate_icds=Input(rid="ri.vector.main.execute.bf34a516-47c9-44b2-a70f-421f24eb5d64")
)
SELECT count(default_ccsr_category_op_clean) as ccsr_count, default_ccsr_category_op_clean
FROM duplicate_icds
group by default_ccsr_category_op_clean
order by ccsr_count desc

