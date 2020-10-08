select * from all_data
where 
	"Классификатор" like '%розыск%' and
	"Период" like 'январь-декабрь' and
	"Категория" like '%Сахалин%';