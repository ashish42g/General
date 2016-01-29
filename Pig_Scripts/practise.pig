student_details = LOAD '/Users/ashish/Documents/Training_docs/pig_training/pig_data/student_details.txt'
USING PigStorage(',')
as (id:int, firstname:chararray, lastname:chararray, age:int, phone:chararray, city:chararray);
employee_details = LOAD '/Users/ashish/Documents/Training_docs/pig_training/pig_data/employee_details.txt' USING PigStorage(',')as (id:int, name:chararray, age:int, city:chararray);
group_data = group student_details by age;
