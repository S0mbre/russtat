drop table if exists Agencies;
drop table if exists Departments;
drop table if exists Classifier;
drop table if exists Codes;
drop table if exists Codevals;
drop table if exists Units;
drop table if exists Periods;
drop table if exists Datasets;
drop table if exists Obs;

create table Agencies(
	id INT GENERATED ALWAYS AS IDENTITY,
	ag_id VARCHAR(16),
	name VARCHAR(512) NOT NULL,
	PRIMARY KEY(id)
);

create table Departments(
	id INT GENERATED ALWAYS AS IDENTITY,
	agency_id INT,
	name VARCHAR(255) NOT NULL,
	PRIMARY KEY(id),
	CONSTRAINT Departments_fk1
		FOREIGN KEY(agency_id) REFERENCES Agencies(id)
		ON DELETE SET NULL ON UPDATE CASCADE
);

create table Classifier(
	id INT GENERATED ALWAYS AS IDENTITY,
	class_id VARCHAR(16),
	name VARCHAR(1024) NOT NULL,
	parent_id INT,
	PRIMARY KEY(id)
);

create table Codes(
	id INT GENERATED ALWAYS AS IDENTITY,
	name VARCHAR(255) NOT NULL,
	PRIMARY KEY(id)
);

create table Codevals(
	id INT GENERATED ALWAYS AS IDENTITY,
	code_id INT,
	val_id VARCHAR(16) NOT NULL,
	name VARCHAR(255) NOT NULL,
	PRIMARY KEY(id),
	CONSTRAINT Codevals_fk1
		FOREIGN KEY(code_id) REFERENCES Codes(id)
		ON DELETE SET NULL ON UPDATE CASCADE
);

create table Units(
	id INT GENERATED ALWAYS AS IDENTITY,
	val VARCHAR(128) NOT NULL,
	PRIMARY KEY(id)
);

create table Periods(
	id INT GENERATED ALWAYS AS IDENTITY,
	val VARCHAR(128) NOT NULL,
	PRIMARY KEY(id)
);

create table Datasets(
	id INT GENERATED ALWAYS AS IDENTITY,
	prep_time TIMESTAMP WITH TIME ZONE,
	updated_time TIMESTAMP WITH TIME ZONE,
	next_update_time TIMESTAMP WITH TIME ZONE,
	ds_id INT,
	agency_id INT,
	dept_id INT,
	name VARCHAR(1024) NOT NULL,
	periodicity VARCHAR(128),
	unit_id INT,
	range_start SMALLINT,
	range_end SMALLINT,
	class_id INT,
	description TEXT,
	prep_by VARCHAR(64),
	prep_contact VARCHAR(512),
	PRIMARY KEY(id),
	CONSTRAINT Datasets_fk1
		FOREIGN KEY(agency_id) REFERENCES Agencies(id)
		ON DELETE SET NULL ON UPDATE CASCADE,
	CONSTRAINT Datasets_fk2
		FOREIGN KEY(dept_id) REFERENCES Departments(id)
		ON DELETE SET NULL ON UPDATE CASCADE,
	CONSTRAINT Datasets_fk3
		FOREIGN KEY(unit_id) REFERENCES Units(id)
		ON DELETE SET NULL ON UPDATE CASCADE,
	CONSTRAINT Datasets_fk4
		FOREIGN KEY(class_id) REFERENCES Classifier(id)
		ON DELETE SET NULL ON UPDATE CASCADE
);

create table Obs(
	id BIGINT GENERATED ALWAYS AS IDENTITY,
	dataset_id INT,
	code_id INT,
	unit_id INT,
	period_id INT,
	obs_year INT,
	obs_val REAL,
	PRIMARY KEY(id),
	CONSTRAINT Obs_fk1
		FOREIGN KEY(dataset_id) REFERENCES Datasets(id)
		ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT Obs_fk2
		FOREIGN KEY(code_id) REFERENCES Codevals(id)
		ON DELETE SET NULL ON UPDATE CASCADE,
	CONSTRAINT Obs_fk3
		FOREIGN KEY(unit_id) REFERENCES Units(id)
		ON DELETE SET NULL ON UPDATE CASCADE,
	CONSTRAINT Obs_fk4
		FOREIGN KEY(period_id) REFERENCES Periods(id)
		ON DELETE SET NULL ON UPDATE CASCADE
);