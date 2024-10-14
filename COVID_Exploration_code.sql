SELECT * 
  FROM `francis-analytics-course.Portfolio_Projects.covid_deaths`
ORDER BY 1;

SELECT 
  location,
  population,
  date,
  total_cases,
  total_deaths 
FROM `francis-analytics-course.Portfolio_Projects.covid_deaths`
ORDER BY location;

SELECT 
  location,
  population,
  date,
  total_cases,
  total_deaths,
CASE
  WHEN total_cases = 0 THEN 0
  ELSE (total_deaths/total_cases) * 100
END AS death_percentage
FROM `francis-analytics-course.Portfolio_Projects.covid_deaths`
ORDER BY location;

SELECT 
  location,
  population,
  date,
  total_deaths,
CASE
  WHEN total_deaths = 0 THEN 0
  ELSE (total_deaths/population) * 100
END AS percentage_of_population_deaths
FROM `francis-analytics-course.Portfolio_Projects.covid_deaths`
ORDER BY location;

SELECT 
  continent,
  MAX(total_deaths) AS total_deaths
FROM 
  `francis-analytics-course.Portfolio_Projects.covid_deaths`
WHERE continent IS NOT NULL
GROUP BY 
  continent
ORDER BY 
  total_deaths DESC;

CREATE VIEW `francis-analytics-course.Portfolio_Projects.deaths_by_continent`
AS
SELECT 
  continent,
  MAX(total_deaths) AS total_deaths
FROM 
  `francis-analytics-course.Portfolio_Projects.covid_deaths`
WHERE continent IS NOT NULL
GROUP BY 
  continent
ORDER BY 
  total_deaths DESC;


SELECT 
  location,
  MAX(total_deaths) AS total_deaths
FROM 
  `francis-analytics-course.Portfolio_Projects.covid_deaths`
GROUP BY 
  location
ORDER BY 
  total_deaths DESC;

SELECT 
  location,
  MAX(total_deaths) AS total_deaths
FROM 
  `francis-analytics-course.Portfolio_Projects.covid_deaths`
WHERE 
  continent IS NOT NULL
GROUP BY 
  location
ORDER BY 
  total_deaths DESC;

CREATE VIEW `francis-analytics-course.Portfolio_Projects.total_deaths_by_location` AS
SELECT 
  location,
  MAX(total_deaths) AS total_deaths
FROM 
  `francis-analytics-course.Portfolio_Projects.covid_deaths`
WHERE 
  continent IS NOT NULL
GROUP BY 
  location
ORDER BY 
  total_deaths DESC;

SELECT 
total_cases,
total_deaths
FROM 
  `francis-analytics-course.Portfolio_Projects.covid_deaths`
WHERE continent = 'Africa'; 


SELECT 
  location,
  MAX(total_cases) AS total_cases,
  MAX(total_deaths) AS total_deaths
FROM 
  `francis-analytics-course.Portfolio_Projects.covid_deaths`
WHERE 
  continent = 'Africa'
GROUP BY location
ORDER BY total_deaths DESC;

CREATE VIEW `francis-analytics-course.Portfolio_Projects.african_cases_vs_death` AS
SELECT 
  location,
  MAX(total_cases) AS total_cases,
  MAX(total_deaths) AS total_deaths
FROM 
  `francis-analytics-course.Portfolio_Projects.covid_deaths`
WHERE 
  continent = 'Africa'
GROUP BY location
ORDER BY total_deaths DESC;

SELECT 
location, 
MAX(new_cases) AS recent_cases,
FROM
  francis-analytics-course.Portfolio_Projects.covid_deaths
WHERE 
  EXTRACT (YEAR FROM date) = 2024 AND continent IS NOT NULL
GROUP BY location
ORDER BY recent_cases DESC;


SELECT 
location, 
MAX(new_cases) AS recent_cases,
FROM
  francis-analytics-course.Portfolio_Projects.covid_deaths
WHERE 
  EXTRACT (YEAR FROM date) = 2024 AND continent IS NOT NULL
GROUP BY location
ORDER BY recent_cases DESC;


SELECT * 
  FROM francis-analytics-course.Portfolio_Projects.covid_vaccinations
ORDER BY 1,2;


SELECT
  location,
  MAX(total_vaccinations) AS total_vaccinations
FROM
  francis-analytics-course.Portfolio_Projects.covid_vaccinations
WHERE
  continent IS NOT NULL
GROUP BY 
  location
ORDER BY 
  total_vaccinations DESC;



SELECT * 
FROM
  `francis-analytics-course.Portfolio_Projects.african_cases_vs_death` DEA
JOIN
  `francis-analytics-course.Portfolio_Projects.covid_vaccinations` VAC
ON
  DEA.location = VAC.location
ORDER BY DEA.location;


SELECT 
  DEA.continent,
  MAX(DEA.population) AS total_population,
  MAX(VAC.total_vaccinations) AS total_vaccinations
FROM
  `francis-analytics-course.Portfolio_Projects.covid_deaths` DEA
JOIN
  `francis-analytics-course.Portfolio_Projects.covid_vaccinations` VAC
ON
  DEA.location = VAC.location
WHERE
  DEA.continent IS NOT NULL
GROUP BY 
  DEA.continent
ORDER BY 
  DEA.continent;

CREATE VIEW `francis-analytics-course.Portfolio_Projects.world_vaccinations` AS
SELECT 
  DEA.continent,
  MAX(DEA.population) AS total_population,
  MAX(VAC.total_vaccinations) AS total_vaccinations
FROM
  `francis-analytics-course.Portfolio_Projects.covid_deaths` DEA
JOIN
  `francis-analytics-course.Portfolio_Projects.covid_vaccinations` VAC
ON
  DEA.location = VAC.location
WHERE
  DEA.continent IS NOT NULL
GROUP BY 
  DEA.continent
ORDER BY 
  DEA.continent;


SELECT
 DEA.continent,
 DEA.location,
 DEA.date,
 DEA.population,
 VAC.new_vaccinations,
 SUM(new_vaccinations) OVER (PARTITION BY DEA.location ORDER BY DEA.date) AS rolling_people_vaccinated
FROM
  `francis-analytics-course.Portfolio_Projects.covid_deaths` DEA
JOIN
  `francis-analytics-course.Portfolio_Projects.covid_vaccinations` VAC
ON
  DEA.location = VAC.location AND
  DEA.date = VAC.date
WHERE
  DEA.continent IS NOT NULL
ORDER BY 
  2,3;



WITH popvsvacc 
AS 
(
SELECT
 DEA.continent,
 DEA.location,
 DEA.date,
 DEA.population,
 VAC.new_vaccinations,
 SUM(new_vaccinations) OVER (PARTITION BY DEA.location ORDER BY DEA.date) AS rolling_people_vaccinated
FROM
  `francis-analytics-course.Portfolio_Projects.covid_deaths` DEA
JOIN
  `francis-analytics-course.Portfolio_Projects.covid_vaccinations` VAC
ON
  DEA.location = VAC.location AND
  DEA.date = VAC.date
WHERE
  DEA.continent IS NOT NULL
)
SELECT *, (rolling_people_vaccinated/population)*100 
FROM popvsvacc;


WITH popvsvacc 
AS 
(
SELECT
 DEA.continent,
 DEA.location,
 DEA.date,
 DEA.population,
 VAC.new_vaccinations,
 SUM(new_vaccinations) OVER (PARTITION BY DEA.location ORDER BY DEA.date) AS rolling_people_vaccinated
FROM
  `francis-analytics-course.Portfolio_Projects.covid_deaths` DEA
JOIN
  `francis-analytics-course.Portfolio_Projects.covid_vaccinations` VAC
ON
  DEA.location = VAC.location AND
  DEA.date = VAC.date
WHERE
  DEA.continent IS NOT NULL
)
SELECT *, (rolling_people_vaccinated/population)*100 AS vacca
FROM popvsvacc
ORDER BY vacca DESC;


CREATE TABLE `francis-analytics-course.Portfolio_Projects.percent_of_population_vacc` AS
(
SELECT
 DEA.continent,
 DEA.location,
 DEA.date,
 DEA.population,
 VAC.new_vaccinations,
 SUM(new_vaccinations) OVER (PARTITION BY DEA.location ORDER BY DEA.date) AS rolling_people_vaccinated
FROM
  `francis-analytics-course.Portfolio_Projects.covid_deaths` DEA
JOIN
  `francis-analytics-course.Portfolio_Projects.covid_vaccinations` VAC
ON
  DEA.location = VAC.location AND
  DEA.date = VAC.date
WHERE 
  DEA.continent IS NOT NULL);

SELECT *, (rolling_people_vaccinated/population)*100 AS vacca
FROM `francis-analytics-course.Portfolio_Projects.percent_of_population_vacc`


CREATE VIEW `francis-analytics-course.Portfolio_Projects.most_vaccinations` AS
SELECT
  location,
  MAX(total_vaccinations) AS total_vaccinations
FROM
  `francis-analytics-course.Portfolio_Projects.covid_vaccinations`
WHERE
  continent IS NOT NULL
GROUP BY 
  location;

SELECT * 
FROM `francis-analytics-course.Portfolio_Projects.most_vaccinations`
