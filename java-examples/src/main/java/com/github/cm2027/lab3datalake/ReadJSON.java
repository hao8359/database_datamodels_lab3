package com.github.cm2027.lab3datalake;

public class ReadJSON {
    public static void main(String[] args) {
        var spark = Session.get();

        var patientsDataFrame = spark
                .read()
                .format("hudi")
                .load(Session.HDFS_BASE_URI + "/fhir/patients-json");
        patientsDataFrame.createOrReplaceTempView("patients");

        System.out.println("===Patients===");
        spark.sql("SELECT * FROM PATIENTS").show();

        System.out.println("===IDs of Patients born on 1971-09-30===");
        spark.sql("SELECT id FROM PATIENTS WHERE birthDate = '1971-09-30'").show(false);

    }
}
