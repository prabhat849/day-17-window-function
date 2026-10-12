
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Day17WindowFunctions {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day17-Window-Functions")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    println("========================================")
    println("DAY 17 - SPARK WINDOW FUNCTIONS")
    println("========================================")

    // 1. Employee dataset: 15 records
    val employees = Seq(
      (1, "Prabhat", 25000.0, "D01"),
      (2, "Rahul", 45000.0, "D02"),
      (3, "Amit", 75000.0, "D01"),
      (4, "Sneha", 120000.0, "D03"),
      (5, "Priya", 60000.0, "D02"),
      (6, "Arjun", 28000.0, "D04"),
      (7, "Neha", 55000.0, "D02"),
      (8, "Karan", 95000.0, "D01"),
      (9, "Anjali", 35000.0, "D03"),
      (10, "Rohit", 85000.0, "D01"),
      (11, "Pooja", 150000.0, "D04"),
      (12, "Vikram", 18000.0, "D05"),
      (13, "Manish", 55000.0, "D02"),
      (14, "Kavita", 68000.0, "D05"),
      (15, "Suresh", 50000.0, "D03")
    ).toDF("employee_id", "employee_name", "salary", "dept_id")

    println("\n===== ORIGINAL EMPLOYEE DATA =====")
    employees.orderBy("employee_id").show(15, false)

    // 2. Window specification: highest salary first per department
    val salaryDescending = Window
      .partitionBy("dept_id")
      .orderBy(col("salary").desc)

    // 3. ROW_NUMBER, RANK and DENSE_RANK
    val rankedEmployees = employees
      .withColumn("row_number", row_number().over(salaryDescending))
      .withColumn("salary_rank", rank().over(salaryDescending))
      .withColumn("dense_rank", dense_rank().over(salaryDescending))

    println("\n===== ROW_NUMBER, RANK AND DENSE_RANK =====")
    rankedEmployees
      .select(
        "dept_id", "employee_name", "salary",
        "row_number", "salary_rank", "dense_rank"
      )
      .orderBy(col("dept_id"), col("salary").desc, col("employee_name"))
      .show(15, false)

    // 4. Highest-paid employee in each department
    println("\n===== HIGHEST-PAID EMPLOYEE IN EACH DEPARTMENT =====")
    rankedEmployees
      .filter(col("row_number") === 1)
      .select("dept_id", "employee_name", "salary")
      .orderBy("dept_id")
      .show(false)

    // 5. Running salary total, lowest salary first
    val salaryAscending = Window
      .partitionBy("dept_id")
      .orderBy(col("salary").asc, col("employee_name").asc)

    val runningWindow = salaryAscending.rowsBetween(
      Window.unboundedPreceding,
      Window.currentRow
    )

    val runningTotals = employees.withColumn(
      "running_salary_total",
      sum(col("salary")).over(runningWindow)
    )

    println("\n===== RUNNING SALARY TOTAL BY DEPARTMENT =====")
    runningTotals
      .select(
        "dept_id", "employee_name", "salary", "running_salary_total"
      )
      .orderBy(col("dept_id"), col("salary").asc, col("employee_name").asc)
      .show(15, false)

    // 6. LAG and LEAD
    val salaryComparison = employees
      .withColumn("previous_salary", lag("salary", 1).over(salaryAscending))
      .withColumn("next_salary", lead("salary", 1).over(salaryAscending))

    println("\n===== LAG AND LEAD SALARY COMPARISON =====")
    salaryComparison
      .select(
        "dept_id", "employee_name", "salary",
        "previous_salary", "next_salary"
      )
      .orderBy(col("dept_id"), col("salary").asc, col("employee_name").asc)
      .show(15, false)

    // 7. Difference from the previous salary
    println("\n===== SALARY DIFFERENCE FROM PREVIOUS EMPLOYEE =====")
    salaryComparison
      .withColumn(
        "salary_difference",
        col("salary") - col("previous_salary")
      )
      .select(
        "dept_id", "employee_name", "salary",
        "previous_salary", "salary_difference"
      )
      .orderBy(col("dept_id"), col("salary").asc, col("employee_name").asc)
      .show(15, false)

    // 8. Summary
    println("\n===== DAY 17 CONCEPTS =====")
    println("row_number: Assigns a unique number to each row.")
    println("rank: Assigns ranks and leaves gaps after ties.")
    println("dense_rank: Assigns ranks without gaps after ties.")
    println("partitionBy: Separates calculations by department.")
    println("lag: Reads a value from the previous row.")
    println("lead: Reads a value from the next row.")
    println("Running total: Calculates cumulative salary.")

    println("\nDAY 17 COMPLETED SUCCESSFULLY")

    spark.stop()
  }
}
