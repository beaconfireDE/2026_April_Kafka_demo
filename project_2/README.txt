Architecture:

DB1: [employees + emp_cdc]
        |
        | (trigger)
        |
     [emp_cdc]
        |
        | (producer)
        |
    [Kafka Topic: employee_cdc_topic]
        |
        |(consumer) ----- invalid action: [kafka topic: bf_employee_cdc_dlq]
        |
DB2:[employees]


 


