select year, category, value from all_data
where
	(dataset like 'Число умерших по основным классам и отдельным причинам смерти за год') and
	(release like '%за год') and
	((code like '%ОКАТО%' and category like 'Росс%') or (code like '%МКБ%'))
group by year, category, value;