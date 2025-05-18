from pyspark.sql.functions import col,row_number
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("USVehicleAccidentAnalysis").getOrCreate()

class USVehicleAccidentAnalysis:
    
    def __init__(self, spark, config):
        
        self.df_damages= spark.read.csv("/Users/senzosenkosishezi/Desktop/WFS/WFS/Data/Damages_use.csv", header=True, inferSchema=True)
        self.df_charges= spark.read.csv("/Users/senzosenkosishezi/Desktop/WFS/WFS/Data/Charges_use.csv", header=True, inferSchema=True)
        self.df_endorse= spark.read.csv("/Users/senzosenkosishezi/Desktop/WFS/WFS/Data/Endorse_use.csv", header=True, inferSchema=True)
        self.df_primary_person= spark.read.csv("/Users/senzosenkosishezi/Desktop/WFS/WFS/Data/Primary_person_use.csv", header=True, inferSchema=True)
        self.df_units= spark.read.csv("/Users/senzosenkosishezi/Desktop/WFS/WFS/Data/Units_use.csv", header=True, inferSchema=True)    
        self.df_restrict= spark.read.csv("/Users/senzosenkosishezi/Desktop/WFS/WFS/Data/Restrict_use.csv", header=True, inferSchema=True)
    
    
    def count_male_accidents(self, output_file_path, file_format):
        """
        Count the number of accidents where the number of
        
        return df.count()
        """
        df =  self.df_primary_person.filter(col("PRSN_GNDR_ID") == "MALE").filter(col("DEATH_CNT") > 2)
        return df.count()
    
    def count_2_wheeler_accidents(self, output_path, output_format):
        df =self.df_units.filter(col("VEH_BODY_STYL_ID").contains( "MOTORCYCLE"))
        return df.count()
        
    
    def top_5_vehicle_makes_for_fatal_crashes_without_airbags(self, output_path, output_format):
        
       df= self.df_units.join(self.df_primary_person,df_units["CRASH_ID"]== self.df_primary_person["CRASH_ID"],"inner").\
        filter(col("PRSN_INJRY_SEV_ID") == "KILLED").filter(col("PRSN_AIRBAG_ID") == "NOT DEPLOYED").filter(col("VEH_MAKE_ID")!= "NA").\
        groupby("VEH_MAKE_ID").\
        count()\
        .orderBy(col("count").desc())\
        .limit(5)
        
       return [row[0] for row in df.collect()]
   
    def count_hit_and_run_with_valid_licenses(self, output_path, output_format):
       hit_and_run = self.df_units.select("CRASH_ID", "VEH_HNR_FL").join(self.df_primary_person.select("CRASH_ID", "DRVR_LIC_TYPE_ID"), on ="CRASH_ID",how="inner")
       # print(hit_and_run.show(5)) 
       hit_and_run = hit_and_run.filter(col("VEH_HNR_FL") == "Y").filter(col("DRVR_LIC_TYPE_ID").isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]))

       return hit_and_run.count()
   
    

    def get_state_with_no_female_accident(self, output_path, output_format):
        state_with_no_female= self.df_primary_person.filter(col("PRSN_GNDR_ID") != "FEMALE").groupby("DRVR_LIC_STATE_ID").count().orderBy(col("count").desc())
        return state_with_no_female.first().DRVR_LIC_STATE_ID
    
    
    def get_top_vehicle_contributing_to_injuries(self, output_path, output_format):
        top_3_to_5= self.df_units.filter(col("VEH_MAKE_ID") != "NA").withColumn("TOT_CASUALTIES_CNT",self.df_units[35] + self.df_units[36]).groupby("VEH_MAKE_ID").sum("TOT_CASUALTIES_CNT")\
        .withColumnRenamed("sum(TOT_CASUALTIES_CNT)", "TOT_CASUALTIES_CNT_AGG").orderBy(col("TOT_CASUALTIES_CNT_AGG").desc())
        df_top_3_to_5 = top_3_to_5.limit(5).subtract(top_3_to_5.limit(2))
        
        return [veh[0] for veh in df_top_3_to_5.select("VEH_MAKE_ID").collect()]
    
    
    def get_top_ethnic_ug_crash_for_each_body_style(self, output_path, output_format):
        
        w = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
        top_ethinic=self.df_units.join(self.df_primary_person,on=["CRASH_ID"],how="inner").filter(~col("VEH_BODY_STYL_ID")\
                .isin(["NA","UNKNOWN","NOT REPORTED","OTHER (EXPLAIN IN NARRATIVE)"]))\
        .filter(~self.df_primary_person.PRSN_ETHNICITY_ID.isin(["NA","UNKNOWN"]))\
        .groupby("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID")\
        .count()\
        .withColumn("row",row_number().over(w))\
        .filter(col("row") == 1)\
        .drop("row","count")    
        
        return top_ethinic
        
    def get_top_5_zip_codes_with_alcohols_as_cf_for_crash(self, output_path, output_format):
        
        get_top_5_zip_codes=self.df_units.join(self.df_primary_person,on=["CRASH_ID"],how="inner").dropna(subset=["DRVR_ZIP"])\
        .filter(col("CONTRIB_FACTR_1_ID").contains("ALCOHOL") |  col("CONTRIB_FACTR_2_ID").contains("ALCOHOL"))\
        .groupby("DRVR_ZIP")\
        .count()\
        .orderBy(col("count").desc())\
        .limit(5)
        
        return[row[0] for row in get_top_5_zip_codes.collect()]
    
    def get_crash_ids_with_no_damage(self, output_path, output_format):
        no_damaged_property= self.df_damages.join(self.df_units,on=["CRASH_ID"], how="inner")\
        .filter(
            (self.df_units.VEH_DMAG_SCL_1_ID > "DAMAGED 4") & (~self.df_units.VEH_DMAG_SCL_1_ID.isin(["NA","NO DAMAGE","IVALID VALUE"]))\
                | (self.df_units.VEH_DMAG_SCL_2_ID > "DAMAGED 4") & (~self.df_units.VEH_DMAG_SCL_2_ID.isin(["NA","NO DAMAGE","IVALID VALUE"]))
        ).filter(self.df_damages.DAMAGED_PROPERTY == "NONE")\
        .filter(self.df_units.FIN_RESP_TYPE_ID =="PROOF OF LIABILITY INSURANCE")\
        .distinct()\
        .count()
        
        return [row[0] for row in no_damaged_property.collect()]
    
    def get_top_5_vehicle_brand(self, output_path, output_format):
        
        top_25_state_list = [
            row[0]
            for row in self.df_units.filter(
                col("VEH_LIC_STATE_ID").cast("int").isNull()
            )
            .groupby("VEH_LIC_STATE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(25)
            .collect()
        ]
        top_10_used_vehicle_colors = [
            row[0]
            for row in self.df_units.filter(self.df_units.VEH_COLOR_ID != "NA")
            .groupby("VEH_COLOR_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(10)
            .collect()
        ]

        df = (
            self.df_charges.join(self.df_primary_person, on=["CRASH_ID"], how="inner")
            .join(self.df_units, on=["CRASH_ID"], how="inner")
            .filter(self.df_charges.CHARGE.contains("SPEED"))
            .filter(
                self.df_primary_person.DRVR_LIC_TYPE_ID.isin(
                    ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]
                )
            )
            .filter(self.df_units.VEH_COLOR_ID.isin(top_10_used_vehicle_colors))
            .filter(self.df_units.VEH_LIC_STATE_ID.isin(top_25_state_list))
            .groupby("VEH_MAKE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
        )
        return [row[0] for row in df.collect()]
    
        
# df =  df_primary_person.filter(col("PRSN_GNDR_ID") == "MALE").filter(col("DEATH_CNT") > 0)
# print(df.count())


# two_wheeler = df_units.filter(col("VEH_BODY_STYL_ID").contains( "MOTORCYCLE"))
# print(two_wheeler.count())


# top_5 = df_units.join(df_primary_person,df_units["CRASH_ID"]== df_primary_person["CRASH_ID"],"inner").\
# filter(col("PRSN_INJRY_SEV_ID") == "KILLED").filter(col("PRSN_AIRBAG_ID") == "NOT DEPLOYED").filter(col("VEH_MAKE_ID")!= "NA").\
# groupby("VEH_MAKE_ID").\
# count()\
# .orderBy(col("count").desc())\
# .limit(5)
# # print(type(top_5))

# # print(top_5.head(5))

# print(top_5.head(2))

# Q4


# Q5
# state_with_no_female= df_primary_person.filter(col("PRSN_GNDR_ID") != "FEMALE").groupby("DRVR_LIC_STATE_ID").count().orderBy(col("count").desc())
# state_with_no_female.show(5)


# Q6

# top_3_to_5= df_units.filter(col("VEH_MAKE_ID") != "NA").withColumn("TOT_CASUALTIES_CNT",df_units[35] + df_units[36]).groupby("VEH_MAKE_ID").sum("TOT_CASUALTIES_CNT")\
#         .withColumnRenamed("sum(TOT_CASUALTIES_CNT)", "TOT_CASUALTIES_CNT_AGG").orderBy(col("TOT_CASUALTIES_CNT_AGG").desc())

# df_top_3_to_5 = top_3_to_5.limit(5).subtract(top_3_to_5.limit(2))


# df_top_3_to_5.show()

# q7

    
# df_primary_person.select(col("PRSN_ETHNICITY_ID")).distinct().show()
# df_units.select(col("VEH_BODY_STYL_ID")).distinct().show()
# top_ethinic.show()

# q8

