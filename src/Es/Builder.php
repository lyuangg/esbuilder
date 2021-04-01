<?php
namespace Yuancode\Es;

use Closure;
use Elasticsearch\Client;
use Exception;

class Builder
{
    protected $indexName = '';
    protected $indexType = '_doc';

    protected $id        = '';
    protected $queryBody = [];

    protected $wheres = [];

    protected $offset   = 0;
    protected $size     = 0;
    protected $sources  = [];
    protected $orders   = [];
    protected $collapse = '';

    protected $groupBy = '';

    protected $client = null;

    public function __construct(Client $client = null)
    {
        $this->client = $client;
    }

    public function setIndex($indexName, $indexType = '_doc')
    {
        $this->indexName = $indexName;
        if ($indexType) {
            $this->indexType = $indexType;
        }
        return $this;
    }

    public function setType($type)
    {
        $this->indexType = $type;
        return $this;
    }

    public function setId($id)
    {
        $this->id = $id;
        return $this;
    }

    public function setBody($body)
    {
        $this->queryBody = $body;
        return $this;
    }

    public function setGroup($by)
    {
        $this->groupBy = $by;
        return $this;
    }

    public function groupBy($by)
    {
        return $this->setGroup($by);
    }

    public function setClient(Client $client)
    {
        $this->client = $client;
        return $this;
    }

    public function addWhere($boolean, $column = '', $operator = '=', $value = null, $boost = 0)
    {
        $operator = trim($operator);
        $boolean  = strtolower($boolean);

        //must_not
        if ($boolean == 'must') {
            if (strpos($operator, '!') === 0) {
                $boolean  = 'must_not';
                $operator = substr($operator, 1);
            }
            if (strpos($operator, 'not') === 0) {
                $boolean  = 'must_not';
                $operator = substr($operator, 3);
            }
        }

        if ($boolean == 'mustnot') {
            $boolean = 'must_not';
        }

        if ($operator == '=' && is_array($value)) {
            $operator = 'in';
        }

        //sub query
        $query = null;
        if ($column instanceof Closure) {
            $boolean  = $value ? strtolower($value) : $boolean;
            $operator = 'sub';
            $query = $this->newQuery();
            call_user_func($column, $query);
        }

        //check boolean
        if (!in_array($boolean, ['must', 'must_not', 'should', 'filter'])) {
            throw new \Exception("boolen($boolean) is error!");
        }

        $this->wheres[$boolean][] = compact('column', 'operator', 'value', 'boost', 'query');
        return $this;
    }

    public function newQuery()
    {
        return new static($this->client);
    }

    public function buildWheres()
    {
        $whereQuerys = [];
        if ($this->wheres) {
            foreach ($this->wheres as $boolean => $wheres) {
                foreach ($wheres as $where) {
                    $whereQuery = $this->buildWhere($where);
                    if ($whereQuery) {
                        if($boolean == 'filter') {
                            if(isset($whereQuery['must']) || isset($whereQuery['must_not']) || isset($whereQuery['should']) || isset($whereQuery['filter'])) {
                                $whereQuerys[$boolean]['bool'] = $whereQuery;
                            } else {
                                $whereQuerys[$boolean][] = $whereQuery;
                            }
                        } else {
                            $whereQuerys[$boolean][] = $whereQuery;
                        }
                    }
                }
            }
            if (count($whereQuerys) > 1) {
                $whereQuerys = ['bool' => $whereQuerys];
            }
        } else {
            $whereQuerys = $this->buildWhere();
        }
        return $whereQuerys;
    }

    public function buildWhere($where = null)
    {
        if ($where) {
            $operator = strtolower($where['operator']);
            if ($operator == 'sub') {
                return $this->subQuery($where);
            } elseif (in_array($operator, ['=', 'term'])) {
                return $this->esTerm($where);
            } elseif (in_array($operator, ['in', 'terms'])) {
                return $this->esTerms($where);
            } elseif (in_array($operator, ['like', 'match', 'multi_match', 'matchs', 'multimatch'])) {
                return $this->esMatch($where);
            } elseif (in_array($operator, ['>', '>=', '<', '<=', 'gt', 'gte', 'lt', 'lte'])) {
                return $this->esRange($where);
            } elseif (in_array($operator, ['exists', 'exist'])) {
                return $this->esExists($where);
            } elseif (in_array($operator, ['miss', 'missing'])) {
                return $this->esMissing($where);
            }
        }
        return $this->esMatchAll();
    }

    protected function subQuery($where)
    {
        $query = $where['query'] ?? null;
        if ($query) {
            return $query->buildWheres();
        }
        return null;
    }

    protected function esTerm($where)
    {
        $boost  = $where['boost'] ?? 0;
        $column = $where['column'] ?: '';
        $value  = $where['value'] ?: '';

        if (empty($column) || empty($value)) {
            return null;
        }

        if ($boost > 0) {
            return [
                'term' => [
                    $column => [
                        'value' => $value,
                        'boost' => $boost,
                    ],
                ],
            ];
        }
        return [
            'term' => [
                $column => $value,
            ],
        ];
    }

    protected function esTerms($where)
    {
        $boost  = $where['boost'] ?? 0;
        $column = $where['column'] ?: '';
        $value  = $where['value'] ?: [];

        if (empty($column) || empty($value)) {
            return null;
        }

        if ($boost > 0) {
            return [
                'terms' => [
                    $column => [
                        'value' => $value,
                        'boost' => $boost,
                    ],
                ],
            ];
        }
        return [
            'terms' => [
                $column => $value,
            ],
        ];
    }

    protected function esMatch($where)
    {
        $boost  = $where['boost'] ?? 0;
        $column = $where['column'] ?: '';
        $value  = $where['value'] ?: [];

        if (empty($column) || empty($value)) {
            return null;
        }

        if ($boost > 0) {
            if (is_array($column)) {
                return [
                    'multi_match' => [
                        'query'  => $value,
                        'fields' => $column,
                        'boost'  => $boost,
                    ],
                ];
            }
            return [
                'match' => [
                    $where['column'] => [
                        'query' => $where['value'],
                        'boost' => $boost,
                    ],
                ],
            ];
        }
        if (is_array($column)) {
            return [
                'multi_match' => [
                    'query'  => $value,
                    'fields' => $column,
                ],
            ];
        }
        return [
            'match' => [
                $where['column'] => $where['value'],
            ],
        ];
    }

    protected function esRange($where)
    {
        $operator = $where['operator'] ?: '';
        $boost    = $where['boost'] ?? 0;
        $column   = $where['column'] ?: '';
        $value    = $where['value'] ?: [];

        if (empty($operator) || empty($column) || empty($value)) {
            return null;
        }

        if (in_array($operator, ['>', 'gt'])) {
            $oper = 'gt';
        } elseif (in_array($operator, ['>=', 'gte'])) {
            $oper = 'gte';
        } elseif (in_array($operator, ['<', 'lt'])) {
            $oper = 'lt';
        } elseif (in_array($operator, ['<=', 'lte'])) {
            $oper = 'lte';
        }

        if ($boost > 0) {
            return [
                'range' => [
                    $column => [
                        $oper   => $value,
                        'boost' => $boost,
                    ],
                ],
            ];
        }
        return [
            'range' => [
                $column => [
                    $oper => $value,
                ],
            ],
        ];
    }

    protected function esExists($where)
    {
        $operator = $where['operator'] ?: '';
        $boost    = $where['boost'] ?? 0;
        $column   = $where['column'] ?: '';

        if (empty($operator) || empty($column)) {
            return null;
        }

        if ($boost > 0) {
            return [
                'exists' => [
                    'field' => $column,
                    'boost' => $boost,
                ],
            ];
        }
        return [
            'exists' => [
                'field' => $column,
            ],
        ];
    }

    protected function esMissing($where)
    {
        $operator = $where['operator'] ?: '';
        $boost    = $where['boost'] ?? 0;
        $column   = $where['column'] ?: '';

        if (empty($operator) || empty($column)) {
            return null;
        }

        if ($boost > 0) {
            return [
                'missing' => [
                    'field' => $column,
                    'boost' => $boost,
                ],
            ];
        }
        return [
            'missing' => [
                'field' => $column,
            ],
        ];
    }

    protected function esMatchAll()
    {
        return ['match_all' => []];
    }

    private function callWhere($boolean, $arguments)
    {
        $operator = '=';
        if (in_array($boolean, ['filter', 'filterIn'])) {
            $boolean = 'filter';
        } elseif (in_array($boolean, ['where', 'whereIn'])) {
            $boolean = 'must';
        } elseif (in_array($boolean, ['orWhere', 'orFilter'])) {
            $boolean = 'should';
        } elseif (in_array($boolean, ['whereNotIn', 'filterNotIn'])) {
            $boolean  = 'must_not';
            $operator = 'in';
        } elseif (in_array($boolean, ['exists', 'exist'])) {
            $boolean  = 'must';
            $operator = 'exists';
        } elseif (in_array($boolean, ['miss', 'missing'])) {
            $boolean  = 'must';
            $operator = 'miss';
        }

        $acount = count($arguments);
        if ($acount == 1) {
            return $this->addWhere($boolean, $arguments[0]);
        } elseif ($acount == 2) {
            return $this->addWhere($boolean, $arguments[0], $operator, $arguments[1]);
        } elseif ($acount == 3) {
            return $this->addWhere($boolean, $arguments[0], $arguments[1], $arguments[2]);
        } elseif ($acount == 4) {
            return $this->addWhere($boolean, $arguments[0], $arguments[1], $arguments[2], $arguments[3]);
        }
        return $this;
    }

    public function callMethod($name, $arguments)
    {
        $method = 'set' . ucfirst($name);
        if (method_exists($this, $method)) {
            return $this->$method(...$arguments);
        }
        return $this->callWhere($name, $arguments);
    }

    public function __call($name, $arguments)
    {
        return $this->callMethod($name, $arguments);
    }

    public static function __callStatic($name, $arguments)
    {
        $instance = new static(null);
        return $instance->callMethod($name, $arguments);
    }

    public function offset($offset)
    {
        $this->offset = intval($offset);
        return $this;
    }

    public function from($offset)
    {
        return $this->offset($offset);
    }

    public function size($size)
    {
        $this->size = intval($size);
        return $this;
    }

    public function take($size)
    {
        return $this->size($size);
    }

    public function limit($from, $size = 0)
    {
        if (func_num_args() == 1) {
            list($size, $from) = [$from, 0];
        }
        return $this->offset($from)->size($size);
    }

    public function select(...$sources)
    {
        $this->sources = $sources;
        return $this;
    }

    public function source(...$sources)
    {
        return $this->select(...$sources);
    }

    public function order($key, $direction = 'asc')
    {
        $this->orders[] = [$key, $direction];
        return $this;
    }

    public function orderBy($key, $direction = 'asc')
    {
        return $this->order($key, $direction);
    }

    public function collapse($field)
    {
        $this->collapse = $field;
        return $this;
    }

    public function distinct($field)
    {
        return $this->collapse($field);
    }

    public function toQuery()
    {
        if (empty($this->indexName)) {
            throw new \Exception("index is empty");
        }

        if ($this->groupBy) {
            return $this->toGroupQuery();
        }

        $body = [];

        if ($this->size) {
            $body['size'] = $this->size;
            $body['from'] = $this->offset;
        }
        if ($this->orders) {
            $body['sort'] = $this->buildOrder();
        }

        $body['query'] = $this->buildWheres();
        if (!isset($body['query']['bool'])) {
            $body['query'] = ['bool' => $body['query']];
        }

        if ($this->sources) {
            $body['_source'] = $this->sources;
        }

        if ($this->collapse) {
            $body['collapse']                              = ['field' => $this->collapse];
            $body['aggs']['count']['cardinality']['field'] = $this->collapse;
        }

        $query = [
            'index' => $this->indexName,
            'type'  => $this->indexType,
        ];

        if ($body) {
            $query['body'] = $body;
        }
        if ($this->queryBody) {
            if (is_array($this->queryBody)) {
                $query['body'] = $this->queryBody;
            } else {
                unset($query['body']);
            }
        }

        if ($this->id) {
            $query['id'] = $this->id;
        }

        return $query;
    }

    protected function toGroupQuery()
    {
        $body          = [];
        $body['from']  = 0;
        $body['size']  = 0;
        $body['query'] = $this->buildWheres();
        if (!isset($body['query']['bool'])) {
            $body['query'] = ['bool' => $body['query']];
        }

        if ($this->collapse) {
            $body['collapse'] = ['field' => $this->collapse];
        }

        if ($this->groupBy) {
            $body['aggs']['groupby']['terms']['field'] = $this->groupBy;
            if ($this->orders) {
                $body['aggs']['groupby']['aggs']['grouplist']['top_hits']['sort'] = $this->buildOrder();
            }
            if ($this->size > 0) {
                $body['aggs']['groupby']['aggs']['grouplist']['top_hits']['size'] = $this->size;
            }
            if ($this->sources) {
                $body['aggs']['groupby']['aggs']['grouplist']['top_hits']['_source']['includes'] = $this->sources;
            }
        }
        $query = [
            'index' => $this->indexName,
            'type'  => $this->indexType,
            'body'  => $body,
        ];

        return $query;
    }

    protected function buildOrder()
    {
        $orders = [];
        if ($this->orders) {
            foreach ($this->orders as $order) {
                list($field, $direction) = $order;
                $orders[]                = [
                    $field => ['order' => $direction],
                ];
            }
        }
        return $orders;
    }

    public function get($haveTotal = false, $mergeSource = true)
    {
        $data = $this->getRaw();

        $total = $data && isset($data['hits']['total']) ? intval($data['hits']['total']) : 0;
        $list  = $data && isset($data['hits']['hits']) ? $data['hits']['hits'] : [];

        //去重 total
        if (isset($data['aggregations']['count']['value'])) {
            $total = intval($data['aggregations']['count']['value']);
        }

        //分组结果
        if (isset($data['aggregations']['groupby']['buckets'])) {
            $total   = count($data['aggregations']['groupby']['buckets']);
            $list    = $data['aggregations']['groupby']['buckets'];
            $newList = [];
            foreach ($list as $groups) {
                $groupId    = $groups['key'];
                $groupTotal = $groups['doc_count'];
                foreach ($groups['grouplist']['hits']['hits'] as $item) {
                    if ($mergeSource && isset($item['_source'])) {
                        $item = array_merge($item, $item['_source']);
                        unset($item['_source']);
                    }
                    $item['total']       = $item['total'] ?? $groupTotal;
                    $newList[$groupId][] = $item;
                }
            }
            return $haveTotal ? [$total, $newList] : $newList;
        }

        if ($mergeSource && $list) {
            $list = array_map(function ($item) {
                $item = array_merge($item, $item['_source']);
                unset($item['_source']);
                return $item;
            }, $list);
        }

        if ($haveTotal) {
            return [$total, $list];
        }
        return $list;
    }

    public function first($mergeSource = true)
    {
        $list = $this->get(false, $mergeSource);
        return $list[0] ?? null;
    }

    public function find($id = '', $mergeSource = true)
    {
        if ($id) {
            $this->setId($id);
        }
        $params = $this->setBody(true)->toQuery();
        if(!isset($params['id'])) {
            throw new \Exception("id is empty");
        }
        $info = $this->client->get($params);
        if ($info && $mergeSource) {
            $info = array_merge($info, $info['_source']);
            unset($info['_source']);
        }
        return $info;
    }

    public function count()
    {
        list($total) = $this->get(true, false);
        return $total;
    }

    public function getRaw()
    {
        $esQuery = $this->toQuery();
        return $this->client->search($esQuery);
    }

    public function insert($rows)
    {
        if (empty($rows)) {
            throw new \Exception("insert empty");
        }

        if (isset($rows[0])) {
            return $this->bulkInsert($rows);
        }

        $params = $this->setBody($rows)->toQuery();

        return $this->client->index($params);
    }

    public function bulkInsert($rows)
    {
        $params = ['body' => []];
        foreach ($rows as $row) {
            $indexArr = [
                'index' => [
                    '_index' => $this->indexName,
                    '_type'  => $this->indexType,
                ],
            ];
            if (isset($row['id']) || isset($row['_id'])) {
                $indexArr['index']['_id'] = $row['id'] ?? $row['_id'];
                unset($row['id']);
                unset($row['_id']);
            }
            $params['body'][] = $indexArr;
            $params['body'][] = $row;
        }
        if (!empty($params['body'])) {
            return $this->client->bulk($params);
        }
        throw new \Exception("insert empty");
    }

    public function update($row, $id = '')
    {
        if ($id) {
            $this->setId($id);
        }
        if ($row) {
            $params = $this->setBody(['doc' => $row])
                ->toQuery();
        } else {
            throw new \Exception("doc is empty");
        }

        if (!isset($params['id'])) {
            throw new \Exception("id not defined");
        }

        return $this->client->update($params);
    }

    public function delete($id = '')
    {
        if ($id) {
            $this->setId($id);
        }
        $params = $this->setBody(true)->toQuery();
        if (!isset($params['id'])) {
            throw new \Exception("id not defined");
        }
        return $this->client->delete($params);
    }
}
