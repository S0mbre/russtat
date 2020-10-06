select * from all_data
where 
	"Год" = 2019 and 
	"Территория" like 'Российская%' and
	("Период" like '%год%' or "Период" = 'январь-декабрь') and
	"Служба" like '%безопасности%'
limit 10000;