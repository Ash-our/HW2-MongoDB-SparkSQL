const students = db.getSiblingDB("university").getCollection("students");
const byLastName = { lastName: 1, firstName: 1, _id: 1 };
const byGpa = { gpa: -1, lastName: 1, firstName: 1, _id: 1 };
const projection = { _id: 1, firstName: 1, lastName: 1, gpa: 1 };

function courseId(input) {
  const value = input.trim();
  const id = Number(value);
  if (!/^[+-]?\d+$/.test(value) || !Number.isInteger(id) || id < -2147483648 || id > 2147483647) {
    throw new Error("Enter a whole-number course ID in the signed 32-bit integer range.");
  }
  return new Int32(id);
}

function minimumGpa(input) {
  const value = input.trim();
  const gpa = Number(value);
  if (!/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?$/i.test(value) ||
      !Number.isFinite(gpa) || gpa < 0 || gpa > 4) {
    throw new Error("Enter a numeric GPA between 0.0 and 4.0.");
  }
  return gpa;
}

function indexScans(plan) {
  if (plan === null || typeof plan !== "object") return [];
  if (plan.stage === "IXSCAN") return [plan];
  return Object.values(plan).flatMap(indexScans);
}

function showMatches(title, filter, sort, explainCourse = false) {
  print(`\n${title}`);
  print(`Filter: ${EJSON.stringify(filter)}`);
  const rows = students.find(filter, projection).sort(sort).toArray();
  print("ID\tFull name\tGPA");
  for (const student of rows) {
    print(`${student._id}\t${student.firstName} ${student.lastName}\t${Number(student.gpa).toFixed(2)}`);
  }
  print(`Matches: ${rows.length}`);

  if (explainCourse) {
    const explained = students.find(filter, projection).sort(sort).explain("executionStats");
    const winning = explained.queryPlanner.winningPlan;
    const scans = indexScans(winning.queryPlan ?? winning);
    print(`courseIds index used: ${scans.some(stage => stage.indexName === "courseIds_1") ? "yes" : "no"}`);
    print("Winning plan:");
    printjson(winning.queryPlan ?? winning);
    print(`Documents examined: ${explained.executionStats.totalDocsExamined}`);
    print(`Index keys examined: ${explained.executionStats.totalKeysExamined}`);
  }
  return rows;
}

function compareLine(key, rows) {
  const values = rows.map(student => [
    student._id, student.firstName, student.lastName, Number(student.gpa).toFixed(2)
  ].join("|"));
  print(`COMPARE ${key}: ${values.join(";")}`);
}

if (process.env.HW2_DEMO === "1") {
  compareLine("course103", showMatches("Part 2 | Course 103", { courseIds: courseId("103") }, byLastName, true));
  compareLine("gpa35", showMatches("Part 3 | GPA > 3.5", { gpa: { $gt: minimumGpa("3.5") } }, byGpa));
  showMatches("Part 3 | Course 103 and GPA > 3.5",
    { courseIds: courseId("103"), gpa: { $gt: minimumGpa("3.5") } }, byGpa);
} else {
  const course = (process.env.HW2_COURSE ?? "").trim();
  const gpa = (process.env.HW2_GPA ?? "").trim();
  if (course && gpa) {
    showMatches("Course and minimum GPA", { courseIds: courseId(course), gpa: { $gt: minimumGpa(gpa) } }, byGpa);
  } else if (course) {
    showMatches("Students by course", { courseIds: courseId(course) }, byLastName, true);
  } else if (gpa) {
    showMatches("Students by GPA", { gpa: { $gt: minimumGpa(gpa) } }, byGpa);
  } else {
    throw new Error("Supply a course ID, a minimum GPA, or both.");
  }
}
