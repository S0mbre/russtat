--explain analyse 
select ds.name "Датасет", ds.description "Описание", obs.obs_year "Год", 
	periods.val "Период", units.val "Единица", codes.name "Рубрика", 
	codevals.name "Категория", obs.obs_val::numeric "Значение", 
	ts_rank(ds.search, to_tsquery('russian', 'стоимость')) "Ранг"
from datasets ds
	join obs on ds.id = obs.dataset_id
	join periods on obs.period_id = periods.id
	join units on obs.unit_id = units.id
	join codevals on obs.code_id = codevals.id
	join codes on codevals.code_id = codes.id
where 
	ds.search @@ to_tsquery('russian', 'стоимость') and
	codevals.search @@ websearch_to_tsquery('russian', '"Российская Федерация" -столица') and
	periods.search @@ to_tsquery('russian', 'год|январь-декабрь')
order by "Ранг" desc, "Датасет" asc, "Год" asc;