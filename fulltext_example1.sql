--explain analyse 
select ds.name "Датасет", ds.description "Описание", obs.obs_year "Год", 
	periods.val "Период", units.val "Единица", codevals.name "Территория",
	obs.obs_val "Значение", 
	ts_rank(ds.search, to_tsquery('russian', 'нефть|газ')) "Ранг"
from datasets ds
	join obs on ds.id = obs.dataset_id
	join periods on obs.period_id = periods.id
	join units on obs.unit_id = units.id
	join codevals on obs.code_id = codevals.id
where 
	ds.search @@ to_tsquery('russian', 'нефть|газ') and
	codevals.search @@ websearch_to_tsquery('russian', '"Российская Федерация" -столица') and
	periods.search @@ to_tsquery('russian', 'год|январь-декабрь')
order by "Ранг" desc, "Датасет" asc, "Год" asc;