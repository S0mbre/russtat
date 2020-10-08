--explain analyse 
SELECT cls.name AS "Классификатор",
		ds.name AS "Наименование",
		ds.updated_time AS "Обновлено",
		ds.prep_time AS "Подготовлено",
		ds.next_update_time AS "Следующее обн.",
		ds.description AS "Описание",
		ag.name AS "Служба",
		dept.name AS "Отдел",
		ds.range_start AS "Нач. год",
		ds.range_end AS "Кон. год",
		ds.prep_by AS "Подготовил",
		ds.prep_contact AS "Контакт",
		ts_rank((ds.search || cls.search || ag.search || dept.search), to_tsquery('russian', 'школа')) as "Ранг"
FROM datasets ds
	JOIN classifier cls ON ds.class_id = cls.id
	JOIN agencies ag ON ds.agency_id = ag.id
	JOIN departments dept ON ds.dept_id = dept.id
WHERE 
	(ds.search || cls.search || ag.search || dept.search) @@ to_tsquery('russian', 'школа')
ORDER BY 
	"Ранг" desc, cls.name, ds.name;