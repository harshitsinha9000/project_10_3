// project 1

REGISTER /usr/local/pig/lib/piggybank.jar

dataset_file =  load 'TitanicData.csv' using org.apache.pig.piggybank.storage.CSVLoader() AS (PassengerId :int,Survived:int,Pclass:int,Name:chararray,Sex:chararray,Age:int,SibSp:int,Parch:int,Ticket:chararray,Fare:int,Cabin:chararray,Embarked:chararray);


//average fare for each class
//query1

dataset_avg = FOREACH (GROUP dataset_file BY Pclass) GENERATE group,AVG(dataset_file.Fare);

//query2
//pepople alive in each class and embarked by Southampton

dataset_alive  = FILTER dataset_file by Survived == 1 AND Embarked matches '[S|s]?';
dataset_alive1 = FOREACH(GROUP dataset_alive BY Pclass) GENERATE group,COUNT(dataset_alive.Name);


//query3
//male and female died in each class

//male
dataset_alive_male  = FILTER dataset_file by Survived ==1 AND Sex matches '(male|Male)?');
dataset_alive_male1 = FOREACH(GROUP dataset_alive_male BY Pclass) GENERATE group,COUNT(dataset_alive_male.Name);
//female
dataset_alive_male  = FILTER dataset_file by Survived ==1 AND Sex matches '(female|Female)?');
dataset_alive_male1 = FOREACH(GROUP dataset_alive_male BY Pclass) GENERATE group,COUNT(dataset_alive_male.Name);




