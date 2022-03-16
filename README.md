# SberDE
Финальный проект по курсу Data Engineer
Для обработки данных об операциях с банковыми картами в хранилище написан скрипт на языке SQL, выполняющий ежедневную выгрузку данных, загружающий ее в хранилище данных и ежедневно строящий отчет о мошеннических операциях на основе анализа загруженных данных.
В скрипте реализована загрузка из файлов отчетов (в .csv и .xlsx), обеспечивающих ежедневное получение новой информации, а так же их обработка совместно с информацией из существующих таблиц СУБД Oracle.
Загрузка данных из файлов отчетов, их предварительная обработка и удаленное взаимодействие с СУБД Oracle происходит посредством скрипта, написанного на  языке программирования Python с использованием таких модулей как Pandas и JayDeBeApi.
Результатом запуска данного скрипта является витрина отчетности по мошенническим операциям, сохраняемая в виде таблицы в СУБД Oracle. 
