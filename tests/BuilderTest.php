<?php

namespace Yuancode\Tests;

use Elasticsearch\ClientBuilder;
use Exception;
use PHPUnit\Framework\TestCase;
use Yuancode\Es\Builder;

class BuilderTest extends TestCase
{
    private $client = null;
    static $index  = 'esbuildertest';
    static $type = '_doc';

    protected function setUp(): void
    {
        $host         = $_SERVER['ES_TEST_HOST'] ?? 'localhost:9200';
        $this->client = ClientBuilder::create()->setHosts([$host])->build();
    }

    public static function setUpBeforeClass(): void
    {
        $host         = $_SERVER['ES_TEST_HOST'] ?? 'localhost:9200';
        $client = ClientBuilder::create()->setHosts([$host])->build();

        $mappings = [
            'properties' => [
                'name' => [
                    'type' => 'keyword',
                ],
                'age' => [
                    'type' => 'integer'
                ],
                'grade' => [
                    'type' => 'keyword',
                ],
                'city' => [
                    'type' => 'keyword',
                ],
                'created' => [
                    'type' => 'date'
                ]
            ]
        ];

        $params = [
            'index' => static::$index,
            'body' => [
                'settings' => [
                    'number_of_shards' => 1,
                    'number_of_replicas' => 1
                ],
                'mappings' => $mappings
            ]
        ];

        try {
            $client->indices()->getSettings(['index'=>static::$index]);
        } catch (\Exception $e) {
            $client->indices()->create($params);
        }
    }

    /**
     *
     * @covers Builder::setIndex
     * @covers Builder::setType
     * @covers Builder::setClient
     *
     */
    public function testEmptyQuery()
    {
        $builder = new Builder($this->client);
        $query   = $builder->index(static::$index, static::$type)
            ->type(static::$type)
            ->toQuery();

        $this->assertContains(static::$type, $query);
        $this->assertContains(static::$index, $query);
    }

    /**
     *
     * @covers Builder::setId
     * @covers Builder::__callStatic
     *
     */
    public function testSetId()
    {
        $query = Builder::client($this->client)
            ->index(static::$index, static::$type)
            ->id(10)
            ->toQuery();
        $this->assertContains(10, $query);
    }

    /**
     *
     * @covers Builder::insert
     *
     */
    public function testInsert()
    {
        $row = [
            'name'  => '张三',
            'age'   => 8,
            'grade' => '一年级',
            'created' => date("Y-m-d"),
        ];
        $result = Builder::client($this->client)
            ->index(static::$index, static::$type)
            ->id('1')
            ->insert($row);

        $this->assertTrue(in_array($result['result'], ['created', 'updated']) ? true : false);

        try {
            Builder::client($this->client)->index(static::$index, static::$type)->insert([]);
        } catch (\Exception $e) {
            $this->assertStringContainsStringIgnoringCase('empty', $e->getMessage());
        }
    }

    /**
     *
     * @depends testInsert
     * @covers Builder::update
     * @covers Builder::setBody
     *
     */
    public function testUpdate()
    {
        $row = [
            'name' => '张三2',
        ];
        $result = Builder::client($this->client)
            ->index(static::$index, static::$type)
            ->update($row, '1');

        $this->assertTrue($result['result'] == 'updated' ? true : false);

        try {
            Builder::client($this->client)->index(static::$index, static::$type)->update([]);
        } catch (\Exception $e) {
            $this->assertStringContainsStringIgnoringCase('empty', $e->getMessage());
        }

        try {
            Builder::client($this->client)->index(static::$index, static::$type)->update($row);
        } catch (\Exception $e) {
            $this->assertStringContainsStringIgnoringCase('defined', $e->getMessage());
        }
    }

    /**
     *
     * @depends testBulkInsert
     * @covers Builder::delete
     * @covers Builder::setBody
     *
     */
    public function testDelete()
    {
        $result = Builder::client($this->client)
            ->index(static::$index, static::$type)
            ->delete('2');

        $this->assertTrue($result['result'] == 'deleted' ? true : false);

        try {
            Builder::client($this->client)->index(static::$index, static::$type)->delete();
        } catch (\Exception $e) {
            $this->assertStringContainsStringIgnoringCase('defined', $e->getMessage());
        }
    }

     /**
      * @covers Builder::insert
      * @covers Builder::bulkInsert
      *
      * @return void
      */
    public function testBulkInsert()
    {
        $rows   = [];
        $grades = ['一年级', '二年级', '三年级'];
        $citys  = ['北京', '上海', '深圳', '广州'];
        for ($i = 1; $i < 11; $i++) {
            $rows[] = [
                'id'    => $i,
                'name'  => '张三' . $i,
                'age'   => rand(7, 20),
                'grade' => $grades[array_rand($grades)],
                'city'  => $citys[array_rand($citys)],
                'created' => date("Y-m-d"),
            ];
        }

        $results = Builder::client($this->client)->index(static::$index, static::$type)->insert($rows);
        $this->assertTrue(count($results['items']) == 10 ? true : false);
    }


    /**
     * @depends testBulkInsert
     * @covers Builder::toQuery
     * @covers Builder::setType
     * @covers Builder::addWhere
     * @covers Builder::buildWheres
     * @covers Builder::buildWhere
     * @covers Builder::callMethod
     * @covers Builder::offset
     * @covers Builder::from
     * @covers Builder::size
     * @covers Builder::limit
     * @covers Builder::select
     * @covers Builder::source
     * @covers Builder::order
     * @covers Builder::orderBy
     * @covers Builder::get
     * @covers Builder::getRaw
     *
     */
    public function testToQuery()
    {
        $result = Builder::client($this->client)
                    ->index(static::$index)
                    ->type(static::$type)
                    ->where('age', '>', 1)
                    ->whereIn('grade', ['一年级', '二年级', '三年级'])
                    ->limit(0, 20)
                    ->source('name', 'age', 'grade', 'city')
                    ->orderBy('age', 'desc')
                    ->order('name', 'desc')
                    ->get();

        $this->assertIsArray($result);
        $this->assertArrayHasKey('name', $result[0]);
    }

    /**
     * @depends testBulkInsert
     * @covers Builder::toQuery
     * @covers Builder::addWhere
     * @covers Builder::buildWheres
     * @covers Builder::buildWhere
     * @covers Builder::callMethod
     * @covers Builder::limit
     * @covers Builder::select
     * @covers Builder::orderBy
     * @covers Builder::get
     * @covers Builder::getRaw
     *
     */
    public function testGet()
    {
        list($count, $result) = Builder::client($this->client)
                    ->index(static::$index)
                    ->type(static::$type)
                    ->where('age', '>', 1)
                    ->limit(0, 20)
                    ->source('name', 'age', 'grade', 'city')
                    ->orderBy('age', 'desc')
                    ->get(true, false);

        $this->assertIsInt($count);
        $this->assertTrue($count > 0 ? true : false);
        $this->assertIsArray($result);
        $this->assertArrayHasKey('_source', $result[0]);
    }


    /**
     * @depends testBulkInsert
     * @covers Builder::first
     * @covers Builder::addWhere
     * @covers Builder::buildWheres
     * @covers Builder::buildWhere
     * @covers Builder::callMethod
     * @covers Builder::take
     * @covers Builder::select
     * @covers Builder::orderBy
     * @covers Builder::get
     * @covers Builder::getRaw
     *
     */
    public function testFirst()
    {
        $result = Builder::client($this->client)
                    ->index(static::$index)
                    ->filter('age', '>', 1)
                    ->filterIn('grade', ['一年级', '二年级', '三年级'])
                    ->take(20)
                    ->select('name', 'age', 'grade', 'city')
                    ->orderBy('age', 'desc')
                    ->first();

        $this->assertIsArray($result);
        $this->assertArrayHasKey('name', $result);
    }

    /**
     * @depends testBulkInsert
     * @covers Builder::find
     * @covers Builder::addWhere
     * @covers Builder::buildWheres
     * @covers Builder::buildWhere
     * @covers Builder::callMethod
     * @covers Builder::take
     * @covers Builder::select
     * @covers Builder::orderBy
     * @covers Builder::get
     * @covers Builder::getRaw
     *
     */
    public function testFind()
    {
        $result = Builder::client($this->client)
                    ->index(static::$index)
                    ->find('1');

        $this->assertIsArray($result);
        $this->assertArrayHasKey('name', $result);

        $result = Builder::client($this->client)
                    ->index(static::$index)
                    ->find('1', false);

        $this->assertArrayHasKey('_source', $result);
    }

    /**
     * @depends testBulkInsert
     * @covers Builder::count
     * @covers Builder::get
     * @covers Builder::getRaw
     * @covers Builder::distinct
     *
     */
    public function testCount()
    {
        $count = Builder::client($this->client)
                    ->index(static::$index)
                    ->where('age','>=', 5)
                    ->count();

        $this->assertIsInt($count);
        $this->assertTrue($count > 0 ? true : false);

        $count = Builder::client($this->client)
                    ->index(static::$index)
                    ->where('age','>=', 5)
                    ->distinct('grade')
                    ->count();

        $this->assertEquals(3, $count);
    }

    /**
     * @depends testBulkInsert
     * @covers Builder::get
     * @covers Builder::collapse
     *
     */
    public function testSubQuery()
    {
        list($count, $list) = Builder::client($this->client)
                    ->index(static::$index)
                    ->filter(function($query) {
                        $query->where('age', '>', 1)
                            ->where('created', '>=', date("Y-m-d"));
                    })
                    ->orWhere('age', '>', 0)
                    ->collapse('grade')
                    ->get(true);

        $this->assertIsInt($count);
        $this->assertEquals($count, 3);
        $this->assertArrayHasKey('name', $list[0]);
    }

    /**
     * @depends testBulkInsert
     * @covers Builder::get
     *
     */
    public function testLike()
    {
        $list = Builder::client($this->client)
                    ->index(static::$index)
                    ->filter(function($query) {
                        $query->where('age', '>', 1)
                            ->where('created', '>=', date("Y-m-d"));
                    })
                    ->where('name', 'like', '张三1')
                    ->get();

        $this->assertArrayHasKey('name', $list[0]);
    }

    /**
     * @depends testBulkInsert
     * @covers Builder::setGroup
     * @covers Builder::groupBy
     *
     */
    public function testGroupBy()
    {
        $list = Builder::client($this->client)
                    ->index(static::$index)
                    ->filter(function($query) {
                        $query->orWhere('age', '>', 1)->orWhere('age', '>', 2);
                    })
                    ->groupBy('city')
                    ->orderBy('age', 'desc')
                    ->take(10)
                    ->get();

        $this->assertArrayHasKey('北京', $list);
        $this->assertArrayHasKey('name', $list['北京'][0]);
    }

    /**
     * @depends testBulkInsert
     * @covers Builder::addWhere
     *
     */
    public function testQueryBoost()
    {
        $list =Builder::client($this->client)
            ->index(static::$index)
            ->where('age', '>', 1)
            ->orWhere('age', '>', 20, 100)
            ->orWhere('age', '>', 10, 50)
            ->orWhere('age', '>', 8, 40)
            ->orderBy('_score', 'desc')
            ->get();

        $this->assertTrue(count($list) > 0 ? true : false);
        $this->assertArrayHasKey('_score', $list[0]);
    }

    /**
     * @covers Builder::toQuery
     */
    public function testFilter()
    {
        $query = Builder::client($this->client)
            ->index(static::$index)
            ->filter('age', '>', 1)
            ->toQuery();

        $this->assertTrue(isset($query['body']['query']['bool']['filter'][0]['range']['age']));
    }
}
