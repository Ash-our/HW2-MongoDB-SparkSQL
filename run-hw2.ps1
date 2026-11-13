param(
    [ValidateSet('All', 'StartMongo', 'Query')]
    [string]$Action = 'All',
    [string]$Course,
    [string]$Gpa
)

$ErrorActionPreference = 'Stop'
$mongoRoot = Join-Path $env:LOCALAPPDATA 'Programs/MongoDB'
$mongoData = Join-Path $env:LOCALAPPDATA 'MongoDB/HW2'
$mongoUri = 'mongodb://127.0.0.1:27028/?serverSelectionTimeoutMS=2000'

function Get-MongoCommand([string]$Name) {
    $command = Get-Command $Name -CommandType Application -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($command) { return $command.Source }
    if (Test-Path -LiteralPath $mongoRoot) {
        $command = Get-ChildItem -LiteralPath $mongoRoot -Recurse -Filter $Name -File |
            Select-Object -First 1
        if ($command) { return $command.FullName }
    }
    throw "Install $Name or put it on PATH."
}

function Start-HomeworkMongo {
    New-Item -ItemType Directory -Path $mongoData -Force | Out-Null
    $listener = [Net.Sockets.TcpClient]::new()
    try {
        try { $listener.Connect('127.0.0.1', 27028) } catch [Net.Sockets.SocketException] {}
        $alreadyRunning = $listener.Connected
    } finally {
        $listener.Dispose()
    }
    $logPath = Join-Path $mongoData 'mongod.log'
    if (-not $alreadyRunning) {
        $mongod = Get-MongoCommand 'mongod.exe'
        $server = Start-Process -FilePath $mongod -ArgumentList @(
            '--dbpath', ('"' + $mongoData + '"'), '--logpath', ('"' + $logPath + '"'),
            '--logappend', '--bind_ip', '127.0.0.1', '--port', '27028'
        ) -WindowStyle Hidden -PassThru
    }

    for ($attempt = 0; $attempt -lt 15; $attempt++) {
        $previousErrorPreference = $ErrorActionPreference
        try {
            $ErrorActionPreference = 'Continue'
            $dbPath = & $mongosh $mongoUri --quiet --eval 'print(db.adminCommand({getCmdLineOpts:1}).parsed.storage.dbPath)' 2>$null
            $mongoExitCode = $LASTEXITCODE
        } finally {
            $ErrorActionPreference = $previousErrorPreference
        }
        if ($mongoExitCode -eq 0) {
            if ($dbPath -ne $mongoData) { throw "Port 27028 points to a different MongoDB data directory: $dbPath" }
            Write-Host 'MongoDB is ready at 127.0.0.1:27028.'
            return
        }
        if ($alreadyRunning -or $server.HasExited) { break }
        Start-Sleep -Milliseconds 500
    }
    throw "MongoDB did not start. See $logPath."
}

Push-Location -LiteralPath $PSScriptRoot
try {
    $mongosh = Get-MongoCommand 'mongosh.exe'
    Start-HomeworkMongo
    if ($Action -eq 'StartMongo') { return }

    if ($Action -eq 'Query') {
        if (-not $PSBoundParameters.ContainsKey('Course') -and -not $PSBoundParameters.ContainsKey('Gpa')) {
            $Course = Read-Host 'Course ID (Enter to skip)'
            $Gpa = Read-Host 'Minimum GPA (Enter to skip)'
        }
        $env:HW2_DEMO = '0'
        $env:HW2_COURSE = $Course
        $env:HW2_GPA = $Gpa
        & $mongosh $mongoUri --quiet --file query-students.js
        if ($LASTEXITCODE -ne 0) { throw 'MongoDB query failed. See the message above.' }
        return
    }

    $sparkHome = [Environment]::GetEnvironmentVariable('SPARK_HOME', 'User')
    if (-not $sparkHome -or -not (Test-Path -LiteralPath (Join-Path $sparkHome 'bin/spark-submit.cmd'))) {
        $sparkHome = $env:SPARK_HOME
    }
    if (-not $sparkHome -or -not (Test-Path -LiteralPath (Join-Path $sparkHome 'bin/spark-submit.cmd'))) {
        throw 'Set SPARK_HOME to the Spark installation folder.'
    }
    $env:SPARK_HOME = $sparkHome
    $java = 'java.exe'
    $jar = 'jar.exe'
    if ($env:JAVA_HOME) {
        $java = Join-Path $env:JAVA_HOME 'bin/java.exe'
        $jar = Join-Path $env:JAVA_HOME 'bin/jar.exe'
    }
    $hadoopHome = Join-Path $env:LOCALAPPDATA 'Hadoop/HW2'
    if (Test-Path -LiteralPath (Join-Path $hadoopHome 'bin/winutils.exe')) {
        $env:HADOOP_HOME = $hadoopHome
    }
    if ($env:HADOOP_HOME) { $env:Path = "${env:HADOOP_HOME}\bin;$env:Path" }
    $env:SPARK_LOCAL_IP = '127.0.0.1'

    Write-Host 'Importing and indexing the students in MongoDB.'
    $importOutput = & $mongosh $mongoUri --quiet --file import-students.js
    if ($LASTEXITCODE -ne 0) { throw 'MongoDB import failed.' }
    $env:HW2_DEMO = '1'
    $mongoOutput = & $mongosh $mongoUri --quiet --file query-students.js
    if ($LASTEXITCODE -ne 0) { throw 'MongoDB demo queries failed.' }

    $tempRoot = (Resolve-Path -LiteralPath ([IO.Path]::GetTempPath())).Path
    $build = Join-Path $tempRoot ("hw2-" + [guid]::NewGuid().ToString('N'))
    $classes = Join-Path $build 'classes'
    $appJar = Join-Path $build 'student-analytics.jar'
    New-Item -ItemType Directory -Path $classes | Out-Null
    try {
        Write-Host 'Compiling Scala and running Spark SQL.'
        & $java -cp (Join-Path $sparkHome 'jars/*') scala.tools.nsc.Main -usejavacp -d $classes StudentAnalytics.scala
        if ($LASTEXITCODE -ne 0) { throw 'Scala compilation failed.' }
        & $jar cf $appJar -C $classes .
        if ($LASTEXITCODE -ne 0) { throw 'Building the application JAR failed.' }
        $sparkSubmit = Join-Path $sparkHome 'bin/spark-submit.cmd'
        $sparkLog = Join-Path $build 'spark.log'
        # Windows PowerShell surfaces native stderr as errors, including JVM startup warnings.
        $previousErrorPreference = $ErrorActionPreference
        try {
            $ErrorActionPreference = 'Continue'
            $sparkOutput = & $sparkSubmit --master 'local[2]' --class StudentAnalytics --conf spark.ui.enabled=false --conf spark.sql.shuffle.partitions=4 $appJar 2>$sparkLog
            $sparkExitCode = $LASTEXITCODE
        } finally {
            $ErrorActionPreference = $previousErrorPreference
        }
        if ($sparkExitCode -ne 0) {
            Get-Content -LiteralPath $sparkLog
            $sparkOutput | Write-Host
            throw 'Spark SQL run failed.'
        }
    } finally {
        $resolvedBuild = (Resolve-Path -LiteralPath $build).Path
        if ((Split-Path -Parent $resolvedBuild) -eq $tempRoot -and
            (Split-Path -Leaf $resolvedBuild) -match '^hw2-[0-9a-f]{32}$') {
            Remove-Item -LiteralPath $resolvedBuild -Recurse -Force
        }
    }

    $comparison = foreach ($key in @('course103', 'gpa35')) {
        $mongoRows = @($mongoOutput | Where-Object { $_.StartsWith("COMPARE ${key}: ") })
        $sparkRows = @($sparkOutput | Where-Object { $_.StartsWith("COMPARE ${key}: ") })
        if ($mongoRows.Count -ne 1 -or $sparkRows.Count -ne 1 -or $mongoRows[0] -cne $sparkRows[0]) {
            throw "MongoDB and Spark results differ for $key."
        }
        if ($key -eq 'course103') { 'Course 103: MongoDB and Spark returned identical rows in the same order.' }
        else { 'GPA > 3.5: MongoDB and Spark returned identical rows in the same order.' }
    }
    @(
        'Homework 2 Student Records with MongoDB and Spark SQL'
        ''
        'Part 1 | Import, indexes and BSON types'
        $importOutput
        ($mongoOutput | Where-Object { -not $_.StartsWith('COMPARE ') })
        ''
        ($sparkOutput | Where-Object { -not $_.StartsWith('COMPARE ') })
        ''
        '4.1 | Cross-check against MongoDB'
        $comparison
    ) | Set-Content -LiteralPath results.txt -Encoding utf8
    Write-Host 'Verified results written to results.txt, students.json and course_stats.parquet.'
} finally {
    Pop-Location
}
