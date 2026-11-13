# Homework 2 Student Records with MongoDB and Spark SQL

MongoDB is a database that stores records as documents. A student document holds the student's name, birth date, GPA, and course IDs. The `mongod` process stores and serves those documents. The `mongosh` shell runs JavaScript commands against the server.

This solution uses JavaScript in `mongosh` for Parts 1 through 3 and Scala with Spark SQL for Part 4. It uses the assignment's Option B. MongoDB stores the birth date as a BSON date. The JSON export represents it as an ISO 8601 string, and Spark reads that field as a timestamp with an explicit schema.

## Run and verify on Windows

الأوامر تحت `powershell` تُكتب في PowerShell. أوامر `javascript` تُكتب بعد فتح MongoDB Shell، وأوامر `scala` تُكتب بعد ظهور `scala>` في Spark Shell.

### 1. Open the project folder

افتح PowerShell وادخل إلى مجلد الواجب. إذا فككت ملف ZIP في مكان آخر، استبدل المسار أدناه بمسار المجلد الذي يحتوي على `run-hw2.ps1`.

```powershell
cd "C:\Users\pc\IdeaProjects\HW2-MongoDB-SparkSQL"
Get-ChildItem
```

The tested setup uses MongoDB Community 8.0.38, MongoDB Shell 2.14.2, Spark 4.2.0, and JDK 25. On this PC, MongoDB and its shell are under `%LOCALAPPDATA%\Programs\MongoDB`, and Spark is at `C:\spark-4.2.0-bin-hadoop3`. On another Windows PC, install the server, shell, Spark, and a JDK compatible with that Spark release. Make `mongod.exe` and `mongosh.exe` available on PATH, and set `SPARK_HOME` and `JAVA_HOME` to your installation directories. If your setup uses Windows Hadoop helpers, set `HADOOP_HOME` to the directory containing their `bin` folder.

### 2. Run all four parts

شغّل هذا الأمر أولًا. سيبدأ MongoDB تلقائيًا إذا كان متوقفًا، ثم يستورد الطلاب وينشئ الفهارس ويشغّل استعلامات MongoDB وتحليلات Spark.

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run-hw2.ps1
```

Wait for `Verified results written to results.txt, students.json and course_stats.parquet.` Then open the results and check the generated files.

```powershell
notepad .\results.txt
Get-Item .\students.json, .\course_stats.parquet
```

The runner uses the local MongoDB instance at `127.0.0.1:27028`, with database files under `%LOCALAPPDATA%\MongoDB\HW2`. Rerunning it rebuilds `university.students` from the supplied CSV and regenerates all three outputs.

### 3. Test the MongoDB queries

نفّذ كل أمر على حدة وقارن عدد النتائج بالجدول أدناه.

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run-hw2.ps1 -Action Query -Course 103
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run-hw2.ps1 -Action Query -Gpa 3.5
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run-hw2.ps1 -Action Query -Course 103 -Gpa 3.5
```

| Filter | Expected output |
| --- | --- |
| Course 103 | `Matches: 36`, `courseIds index used: yes`, and `IXSCAN` on `courseIds_1` |
| GPA strictly above 3.5 | `Matches: 49` |
| Course 103 and GPA strictly above 3.5 | `Matches: 9` |

لإدخال القيم بنفسك، شغّل الأمر التالي. اكتب `103` عند طلب رقم المساق و`3.5` عند طلب المعدل. اضغط Enter لتجاوز أحد الحقلين عند استخدام شرط واحد. جرّب `103.2` كرقم مساق أو `4.1` كمعدل للتأكد من أن البرنامج يرفض القيم غير الصالحة.

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run-hw2.ps1 -Action Query
```

### 4. Inspect the MongoDB collection directly

يمكنك فتح MongoDB Shell للتأكد من عدد السجلات والفهارس. شغّل الأمرين التاليين في PowerShell.

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run-hw2.ps1 -Action StartMongo
mongosh.exe "mongodb://127.0.0.1:27028/university"
```

If PowerShell cannot find `mongosh.exe` on this PC, open a new PowerShell window to pick up the updated PATH or run it by its installed path.

```powershell
& "$env:LOCALAPPDATA\Programs\MongoDB\mongosh-2.14.2-win32-x64\bin\mongosh.exe" "mongodb://127.0.0.1:27028/university"
```

Once connected, run these commands inside MongoDB Shell.

```javascript
db.students.countDocuments({})
db.students.getIndexes()
db.students.findOne({ _id: 1 })
exit
```

Expect 100 documents and three indexes named `_id_`, `courseIds_1`, and `gpa_1`.

### 5. Open Spark Shell

ارجع إلى PowerShell. شغّل الأوامر التالية من مجلد الواجب بعد تنفيذ الخطوة 2، ثم انتظر حتى تظهر `scala>`. المسارات أدناه تخص هذا الجهاز.

```powershell
$env:SPARK_HOME = 'C:\spark-4.2.0-bin-hadoop3'
$env:HADOOP_HOME = "$env:LOCALAPPDATA\Hadoop\HW2"
$env:Path = "${env:HADOOP_HOME}\bin;$env:Path"
$env:SPARK_LOCAL_IP = '127.0.0.1'
& "$env:SPARK_HOME\bin\spark-shell.cmd" --master "local[2]" --conf spark.ui.enabled=false --conf spark.sql.catalogImplementation=in-memory
```

### 6. Inspect the Spark tables and Parquet output

انسخ كل سطر داخل Spark Shell. هذه الخطوات تعيد إنشاء الجدولين المؤقتين في الجلسة الحالية.

```scala
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.session.timeZone", "UTC")
val schema = "_id INT, firstName STRING, lastName STRING, dateOfBirth TIMESTAMP, courseIds ARRAY<INT>, gpa DOUBLE"
val students = spark.read.schema(schema).json("students.json")
students.printSchema()
students.createOrReplaceTempView("students")
spark.sql("SELECT _id, firstName, lastName, gpa, explode(courseIds) AS course FROM students").createOrReplaceTempView("enrollments")
spark.sql("SHOW TABLES").show(false)
```

The schema should show `dateOfBirth` as a timestamp and `courseIds` as an array of integers. `SHOW TABLES` should list `students` and `enrollments` with `isTemporary = true`. These views are available for the current Spark session. The Parquet dataset remains on disk.

```scala
spark.sql("SELECT COUNT(*) AS student_count FROM students").show()
spark.sql("SELECT COUNT(*) AS enrollment_count FROM enrollments").show()
spark.sql("SELECT COUNT(*) AS course_103_count FROM students WHERE array_contains(courseIds, 103)").show()
spark.sql("SELECT COUNT(*) AS gpa_above_3_5_count FROM students WHERE gpa > 3.5").show()
spark.read.parquet("course_stats.parquet").orderBy(org.apache.spark.sql.functions.desc("student_count")).show(false)
:quit
```

توقّع ظهور **100** طالب، و**218** تسجيلًا، و**36** طالبًا في المساق 103، و**49** طالبًا بمعدل أعلى من 3.5، و**5** صفوف في ملخّص المساقات. لمراجعة كل استعلامات الواجب ونتائجها افتح `results.txt`.

### If a command fails

| Message or symptom | What to check |
| --- | --- |
| PowerShell cannot find `run-hw2.ps1` | Use `Get-Location` and `Get-ChildItem` to confirm that you are in the extracted project folder. |
| `Set SPARK_HOME to the Spark installation folder` | Point `SPARK_HOME` to the folder that contains `bin\spark-submit.cmd`. |
| `MongoDB did not start` | Read the startup log with `Get-Content "$env:LOCALAPPDATA\MongoDB\HW2\mongod.log" -Tail 20`. |
| Spark cannot find `students.json` or `course_stats.parquet` | Run step 2 and launch Spark Shell from the project folder. |

## Results and interpretation

- MongoDB imported 100 students. The `courseIds_1` index is multikey because `courseIds` is an array. For course 103, the winning plan used an `IXSCAN` on that index, fetched 36 matching documents, and sorted them by name. The single-field `gpa_1` index supports numeric range lookups such as `gpa > 3.5`.
- Course 103 returned 36 students. GPA strictly above 3.5 returned 49. Applying both filters returned 9. Spark and MongoDB returned identical rows in the same order for the two queries in Part 4.1.
- The Spark queries scan the JSON data and use SQL to expand the arrays into 218 enrollments, then group, join, and rank the rows. Eight students take one course, 66 take two, and 26 take three. Seven pairs share a full name.
- `DENSE_RANK` assigns the same rank to students with equal GPAs. The output includes everyone at each of the three highest distinct GPA levels for their course, so a course can show more than three students.
- To find students above the mean in every course they take, the query compares each student's GPA against each course's exact mean using GPA totals and enrollment counts. Forty-nine students passed every comparison.
- Spark read back all five rows from Parquet and verified that they match the rows in the course summary.
