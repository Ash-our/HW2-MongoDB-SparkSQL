const fs = require("fs");
const { randomUUID } = require("crypto");

function parseStudent(line, row) {
  const fields = line.match(/^([^,]+),([^,]+),([^,]+),(\d{4}-\d{2}-\d{2}),\[([^\]]*)\],([^,]+)$/);
  if (!fields) throw new Error(`Malformed CSV row ${row}`);
  const [, id, firstName, lastName, birthday, courses, gpaText] = fields;
  const int32 = value => {
    const token = value.trim();
    const number = Number(token);
    if (!/^\d+$/.test(token) || !Number.isInteger(number) || number > 2147483647) {
      throw new Error(`Invalid integer "${token}" on row ${row}`);
    }
    return new Int32(number);
  };
  const dateOfBirth = new Date(`${birthday}T00:00:00.000Z`);
  if (Number.isNaN(dateOfBirth.getTime()) || dateOfBirth.toISOString().slice(0, 10) !== birthday) {
    throw new Error(`Invalid date on row ${row}`);
  }
  const gpa = Number(gpaText);
  if (!gpaText.trim() || !Number.isFinite(gpa) || gpa < 0 || gpa > 4) {
    throw new Error(`Invalid GPA on row ${row}`);
  }
  return {
    _id: int32(id),
    firstName: firstName.trim(),
    lastName: lastName.trim(),
    dateOfBirth,
    courseIds: courses.trim() ? courses.split(",").map(int32) : [],
    gpa: new Double(gpa)
  };
}

const [header, ...lines] = fs.readFileSync("hw2-data.csv", "utf8")
  .replace(/^\uFEFF/, "").trimEnd().split(/\r?\n/);
if (header !== "ID,FirstName,LastName,DateOfBirth,CourseIDs,GPA" || lines.length === 0) {
  throw new Error("Expected the assignment CSV with its header and at least one student.");
}
const documents = lines.map((line, index) => parseStudent(line, index + 2));
if (new Set(documents.map(student => student._id.valueOf())).size !== documents.length) {
  throw new Error("The CSV contains duplicate student IDs.");
}

const university = db.getSiblingDB("university");
const staging = university.getCollection(`students_import_${randomUUID()}`);

// Replace the assignment collection only after the new data and indexes are ready.
try {
  staging.insertMany(documents);
  staging.createIndex({ courseIds: 1 });
  staging.createIndex({ gpa: 1 });
  staging.renameCollection("students", true);
} catch (error) {
  staging.drop();
  throw error;
}

const students = university.getCollection("students");
const stored = students.find().sort({ _id: 1 }).toArray();
const jsonLines = stored.map(student => EJSON.serialize(student, { relaxed: true }))
  .map(student => JSON.stringify({ ...student, dateOfBirth: student.dateOfBirth.$date }));
fs.writeFileSync("students.json", jsonLines.join("\n") + "\n", "utf8");
print(`Inserted ${stored.length} documents into university.students.`);
print("Indexes:");
printjson(students.getIndexes());

const schemaCheck = students.aggregate([
  { $project: {
    _id: 0,
    idType: { $type: "$_id" },
    firstNameType: { $type: "$firstName" },
    lastNameType: { $type: "$lastName" },
    birthDateType: { $type: "$dateOfBirth" },
    coursesType: { $type: "$courseIds" },
    courseItemTypes: { $setUnion: [{ $map: {
      input: "$courseIds", as: "id", in: { $type: "$$id" }
    } }, []] },
    gpaType: { $type: "$gpa" }
  } },
  { $group: { _id: "$$ROOT", documents: { $sum: 1 } } }
]).toArray();
print("Stored BSON types:");
printjson(schemaCheck);
