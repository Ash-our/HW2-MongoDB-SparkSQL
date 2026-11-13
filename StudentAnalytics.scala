import java.util.Locale
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

object StudentAnalytics {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HW2 Student Analytics")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    try {
      spark.sparkContext.setLogLevel("ERROR")
      analyze(spark)
    } finally {
      spark.stop()
    }
  }

  private def analyze(spark: SparkSession): Unit = {
    val schema = new StructType()
      .add("_id", IntegerType)
      .add("firstName", StringType)
      .add("lastName", StringType)
      .add("dateOfBirth", TimestampType)
      .add("courseIds", ArrayType(IntegerType))
      .add("gpa", DoubleType)

    val students = spark.read.schema(schema).option("mode", "FAILFAST").json("students.json")
    println("Part 4 | students.json schema")
    students.printSchema()
    println(s"Students loaded: ${students.count()}")
    students.createOrReplaceTempView("students")

    def run(title: String, sql: String): DataFrame = {
      val statement = sql.stripMargin.trim
      println(s"\n$title\n$statement")
      val result = spark.sql(statement)
      result.show(Int.MaxValue, truncate = false)
      result
    }

    def comparison(label: String, result: DataFrame): Unit = {
      val values = result.collect().map { row =>
        val gpa = String.format(Locale.ROOT, "%.2f", Double.box(row.getDouble(3)))
        s"${row.getInt(0)}|${row.getString(1)}|${row.getString(2)}|$gpa"
      }
      println(s"Matches: ${values.length}")
      println(s"COMPARE $label: ${values.mkString(";")}")
    }

    val course103 = run("4.1 | Students in course 103",
      """SELECT _id, firstName, lastName, gpa
        |FROM students
        |WHERE array_contains(courseIds, 103)
        |ORDER BY lastName, firstName, _id""")
    comparison("course103", course103)

    val gpa35 = run("4.1 | Students with GPA > 3.5",
      """SELECT _id, firstName, lastName, gpa
        |FROM students
        |WHERE gpa > 3.5
        |ORDER BY gpa DESC, lastName, firstName, _id""")
    comparison("gpa35", gpa35)

    val enrollments = run("4.2 | Enrollments",
      """SELECT _id, firstName, lastName, gpa, explode(courseIds) AS course
        |FROM students
        |ORDER BY _id, course""")
    enrollments.createOrReplaceTempView("enrollments")
    println(s"Enrollment rows: ${enrollments.count()}")

    val courseSummary = run("4.2 | Course summary",
      """SELECT course, COUNT(*) AS student_count,
        |       ROUND(AVG(CAST(gpa AS DECIMAL(8, 4))), 2) AS avg_gpa,
        |       MIN(gpa) AS min_gpa, MAX(gpa) AS max_gpa
        |FROM enrollments
        |GROUP BY course
        |ORDER BY student_count DESC, course""")

    run("4.2 | Number of courses per student",
      """SELECT size(courseIds) AS course_count, COUNT(*) AS student_count
        |FROM students
        |WHERE size(courseIds) BETWEEN 1 AND 3
        |GROUP BY size(courseIds)
        |ORDER BY course_count""")

    run("4.3 | Students and average GPA by birth year",
      """SELECT year(dateOfBirth) AS birth_year, COUNT(*) AS student_count,
        |       AVG(CAST(gpa AS DECIMAL(8, 4))) AS avg_gpa
        |FROM students
        |GROUP BY year(dateOfBirth)
        |ORDER BY birth_year""")

    run("4.3 | Top three GPA ranks for each course",
      """WITH ranked AS (
        |    SELECT course, _id, firstName, lastName, gpa,
        |           DENSE_RANK() OVER (PARTITION BY course ORDER BY gpa DESC) AS gpa_rank
        |    FROM enrollments
        |)
        |SELECT course, gpa_rank, _id, firstName, lastName, gpa
        |FROM ranked
        |WHERE gpa_rank <= 3
        |ORDER BY course, gpa_rank, lastName, firstName, _id""")
    println("Equal GPAs share a rank. Each course includes all students in its three highest distinct GPA ranks.")

    run("4.3 | Pairs of students with the same full name",
      """SELECT a.firstName, a.lastName,
        |       a._id AS first_id, CAST(a.dateOfBirth AS DATE) AS first_birth_date,
        |       b._id AS second_id, CAST(b.dateOfBirth AS DATE) AS second_birth_date
        |FROM students AS a
        |JOIN students AS b
        |  ON a.firstName = b.firstName AND a.lastName = b.lastName AND a._id < b._id
        |ORDER BY a.lastName, a.firstName, a._id, b._id""")

    run("4.3 | Students above the average GPA of every course they take",
      """WITH course_totals AS (
        |    SELECT course, SUM(CAST(gpa AS DECIMAL(8, 4))) AS total_gpa,
        |           COUNT(*) AS student_count
        |    FROM enrollments
        |    GROUP BY course
        |)
        |SELECT e._id, e.firstName, e.lastName, e.gpa, COUNT(*) AS courses_taken
        |FROM enrollments AS e
        |JOIN course_totals AS c ON e.course = c.course
        |GROUP BY e._id, e.firstName, e.lastName, e.gpa
        |HAVING MIN(CAST(e.gpa AS DECIMAL(8, 4)) * c.student_count - c.total_gpa) > 0
        |ORDER BY e.gpa DESC, e.lastName, e.firstName, e._id""")
    println("GPA times enrollment must exceed total GPA in every course. This compares against the course mean before rounding.")

    println("\n4.4 | Parquet write and readback")
    val parquetPath = "course_stats.parquet"
    courseSummary.write.mode("overwrite").parquet(parquetPath)
    val readback = spark.read.parquet(parquetPath)
    readback.orderBy(readback("student_count").desc, readback("course")).show(truncate = false)
    require(
      courseSummary.exceptAll(readback).take(1).isEmpty &&
        readback.exceptAll(courseSummary).take(1).isEmpty,
      "Parquet readback differs from the course summary."
    )
    println(s"Parquet readback verified: ${readback.count()} rows match the course summary.")
  }
}
