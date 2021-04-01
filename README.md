esbuilder 提供一个 Elasticsearch 的查询构造器, 像使用 Laravel 一样.

## 安装

```
composer require 'yuancode/esbuilder'
```

## 使用

### 创建一个 es client

```php
use Elasticsearch\ClientBuilder;
use Yuancode\Es\Builder;

$client = ClientBuilder::create()->setHosts(['localhost:9200'])->build();

$query = Builder::client($client);
```

### 设置索引

```php
Builder::client($client)->index('testindex');
```

### 插入

```php
$row = [
    'name'  => '张三',
    'age'   => 8,
    'created' => date("Y-m-d"),
];
$result = Builder::client($client)
    ->index('testes')
    ->id('1')  //设置id
    ->insert($row);
```

### 批量插入

```php
$rows   = [];
$grades = ['一年级', '二年级', '三年级'];
for ($i = 1; $i < 10; $i++) {
    $rows[] = [
        'id'    => $i,
        'name'  => '张三' . $i,
        'age'   => rand(7, 20),
        'grade' => $grades[array_rand($grades)],
    ];
}
$results = Builder::client($client)->index('testes')->insert($rows);
```

### 更新

```php
$row = [
    'name' => '张三2',
];
$result = Builder::client($client)
    ->index('testes')
    ->update($row, '1');
```

### 删除

```php
$result = Builder::client($client)
    ->index('testes')
    ->delete('1');
```

### 简单查询

```php
$info = Builder::client($client)
    ->index('testes')
    ->find('1');
```

### 条件查询

```php
$arr = Builder::client($client)
    ->index('testes')
    ->where('id', '1');
    ->get();
```

### 过滤查询

```php
$arr = Builder::client($client)
    ->index('testes')
    ->filter('id', '1');
    ->get();
```

### 其他查询条件

```php
$arr = Builder::client($client)
    ->index('testes')
    ->filter('id', '1');
    ->where('age', '>', 10)
    ->where('age', '!=', 9)
    ->whereIn('id', [1,2,3])
    ->whereNotIn('id', [4,5,6])
    ->where('name', 'like', 'test') // match
    ->orWhere('name', 'zhangsna')
    ->get();
```

### 排序,去重

```php
$arr = Builder::client($client)
    ->index('testes')
    ->where('age', '>', 10)
    ->orderBy('id', 'desc')
    ->distinct('name')
    ->select('name', 'age')
    ->get();
```

### 返回总数量

```php
list($total, $arr) = Builder::client($client)
    ->index('testes')
    ->where('age', '>', 10)
    ->get(true);
// or
$total = Builder::client($client)->index('testes')->where('age', 10)->count();
```

### 指定数量

```php
$arr = Builder::client($client)
    ->index('testes')
    ->where('age', '>', 10)
    ->from(0)
    ->take(10)
    ->get();

//使用 limit 
$arr = Builder::client($client)
    ->index('testes')
    ->where('age', '>', 10)
    ->limit(0, 10)
    ->get();
```

### 使用 boolen 操作

```php
$arr = Builder::client($client)
    ->index('testes')
    ->filter('name', 's')
    ->must('aaa', 'bbb')
    ->must('aaa2', 'term', 'bbb')
    ->mustNot('ccc', 'dddd')
    ->should('aaa','sdlfks')
    ->get();
```

### 嵌套查询

```php
$arr = Builder::client($client)
    ->index('testes')
    ->filter(function($query) {
        $query->where('name2', 'z')
            ->where('name3', 'a');
    })
    ->where('name', 'like', 'zhangsan')
    ->from(0)
    ->take(10)
    ->get();
//filter, where, orWhere, must, mustNot,should, 都支持嵌套查询
```

### 分组查询 topN 的数据

```php
$arr = Builder::client($client)
    ->index('testes')
    ->groupBy('name')
    ->orderBy('age', 'desc')
    ->take(10)
    ->get();
```

### 返回查询

```php
$query = Builder::client($client)
    ->index('testes')
    ->where('name', 'lskdjfls')
    ->take(10)
    ->toQuery();

print_r($query);
```

### boost, _score

```php
$query = Builder::client($client)
    ->index('testes')
    ->orWhere('name', '=', 'aaa', 10)
    ->orWhere('name', '=', 'bbb', 20)
    ->orderBy('_score', 'desc')
    ->take(10)
    ->get();
```
