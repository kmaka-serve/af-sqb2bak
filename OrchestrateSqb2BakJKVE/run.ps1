param($Context)

$in = $Context.Input

# 1) Викликаємо activity і ЯВНО чекаємо результат
$task   = Invoke-DurableActivity -FunctionName 'Worker_JKVE' -Input $in
$result = Wait-DurableTask -Task $task

# 2) Щоб одразу бачити відповідь у статусі
Set-DurableCustomStatus -Value $result

# 3) Повертаємо в output (буде видно в statusQueryGetUri)
return $result