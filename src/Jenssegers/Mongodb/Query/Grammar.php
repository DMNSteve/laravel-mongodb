<?php

namespace Jenssegers\Mongodb\Query;

use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Grammars\Grammar as BaseGrammar;
use Illuminate\Support\Str;
use MongoDB\BSON\ObjectId;

class Grammar extends BaseGrammar
{
    /**
     * Compile a select statement
     *
     * @param  Builder  $builder
     * @return array
     */
    public function compileSelect(Builder $builder): array
    {
        return [
            'collection' => $builder->from,
            'query'      => $this->compileWheres($builder),
            'options'    => $this->compileOptions($builder),
        ];
    }

    /**
     * Compile a distinct query
     *
     * @param Builder $builder
     * @return array
     */
    public function compileDistinct(Builder $builder): array
    {
        return [
            'collection' => $builder->from,
            'column'     => $builder->columns[0] ?? '_id',
            'query'      => $this->compileWheres($builder),
        ];
    }

    /**
     * Compile a distinct query
     *
     * @param Builder $builder
     * @return array
     */
    public function compileAggregate(Builder $builder, $aggregate): array
    {
        $group = [];
        $unwinds = [];

        // Add grouping columns to the $group part of the aggregation pipeline.
        if ($builder->groups) {
            foreach ($builder->groups as $column) {
                $group['_id'][$column] = '$' . $column;

                // When grouping, also add the $last operator to each grouped field,
                // this mimics MySQL's behaviour a bit.
                $group[$column] = ['$last' => '$' . $column];
            }

            // Do the same for other columns that are selected.
            foreach ($builder->columns as $column) {
                $key = str_replace('.', '_', $column);

                $group[$key] = ['$last' => '$' . $column];
            }
        }

        // Add aggregation functions to the $group part of the aggregation pipeline,
        // these may override previous aggregations.
        if ($builder->aggregate) {
            $function = $builder->aggregate['function'];

            foreach ($builder->aggregate['columns'] as $column) {
                // Add unwind if a subdocument array should be aggregated
                // column: subarray.price => {$unwind: '$subarray'}
                if (count($splitColumns = explode('.*.', $column)) == 2) {
                    $unwinds[] = $splitColumns[0];
                    $column = implode('.', $splitColumns);
                }

                // Translate count into sum.
                if ($function == 'count') {
                    $group['aggregate'] = ['$sum' => 1];
                } // Pass other functions directly.
                else {
                    $group['aggregate'] = ['$' . $function => '$' . $column];
                }
            }
        }

        // When using pagination, we limit the number of returned columns
        // by adding a projection.
        if ($builder->paginating) {
            foreach ($builder->columns as $column) {
                $builder->projections[$column] = 1;
            }
        }

        // The _id field is mandatory when using grouping.
        if ($group && empty($group['_id'])) {
            $group['_id'] = null;
        }

        // Build the aggregation pipeline.
        $pipeline = [];

        if ($wheres = $this->compileWheres($builder)) {
            $pipeline[] = ['$match' => $wheres];
        }

        // apply unwinds for subdocument array aggregation
        foreach ($unwinds as $unwind) {
            $pipeline[] = ['$unwind' => '$' . $unwind];
        }

        if ($group) {
            $pipeline[] = ['$group' => $group];
        }

        if ($builder->orders) {
            $pipeline[] = ['$sort' => $builder->orders];
        }

        if ($builder->offset) {
            $pipeline[] = ['$skip' => $builder->offset];
        }

        if ($builder->limit) {
            $pipeline[] = ['$limit' => $builder->limit];
        }

        if ($builder->projections) {
            $pipeline[] = ['$project' => $builder->projections];
        }

        return [
            'collection' => $builder->from,
            'pipeline'   => $pipeline,
            'options'    => $this->compileOptions($builder),
        ];
    }

    /**
     * Convert a key to ObjectID if needed.
     *
     * @param  mixed $id
     * @return mixed
     */
    public function convertKey($id)
    {
        if (is_string($id) && strlen($id) === 24 && ctype_xdigit($id)) {
            return new ObjectId($id);
        }

        return $id;
    }

    /**
     * Compile the where array.
     *
     * @return array
     */
    protected function compileWheres(Builder $query)
    {
        // The wheres to compile.
        $wheres = $query->wheres ?: [];

        // We will add all compiled wheres to this array.
        $compiled = [];

        foreach ($wheres as $i => &$where) {
            // Make sure the operator is in lowercase.
            if (isset($where['operator'])) {
                $where['operator'] = strtolower($where['operator']);

                // Operator conversions
                $convert = [
                    'regexp' => 'regex',
                    'elemmatch' => 'elemMatch',
                    'geointersects' => 'geoIntersects',
                    'geowithin' => 'geoWithin',
                    'nearsphere' => 'nearSphere',
                    'maxdistance' => 'maxDistance',
                    'centersphere' => 'centerSphere',
                    'uniquedocs' => 'uniqueDocs',
                ];

                if (array_key_exists($where['operator'], $convert)) {
                    $where['operator'] = $convert[$where['operator']];
                }
            }

            // Convert id's.
            if (isset($where['column']) && ($where['column'] == '_id' || Str::endsWith($where['column'], '._id'))) {
                // Multiple values.
                if (isset($where['values'])) {
                    foreach ($where['values'] as &$value) {
                        $value = $this->convertKey($value);
                    }
                } // Single value.
                elseif (isset($where['value'])) {
                    $where['value'] = $this->convertKey($where['value']);
                }
            }

            // Convert DateTime values to UTCDateTime.
            if (isset($where['value'])) {
                if (is_array($where['value'])) {
                    array_walk_recursive($where['value'], function (&$item, $key) {
                        if ($item instanceof DateTime) {
                            $item = new UTCDateTime($item->getTimestamp() * 1000);
                        }
                    });
                } else {
                    if ($where['value'] instanceof DateTime) {
                        $where['value'] = new UTCDateTime($where['value']->getTimestamp() * 1000);
                    }
                }
            } elseif (isset($where['values'])) {
                array_walk_recursive($where['values'], function (&$item, $key) {
                    if ($item instanceof DateTime) {
                        $item = new UTCDateTime($item->getTimestamp() * 1000);
                    }
                });
            }

            // The next item in a "chain" of wheres devices the boolean of the
            // first item. So if we see that there are multiple wheres, we will
            // use the operator of the next where.
            if ($i == 0 && count($wheres) > 1 && $where['boolean'] == 'and') {
                $where['boolean'] = $wheres[$i + 1]['boolean'];
            }

            // We use different methods to compile different wheres.
            $method = "compileWhere{$where['type']}";
            $result = $this->{$method}($where);

            // Wrap the where with an $or operator.
            if ($where['boolean'] == 'or') {
                $result = ['$or' => [$result]];
            }

            // If there are multiple wheres, we will wrap it with $and. This is needed
            // to make nested wheres work.
            elseif (count($wheres) > 1) {
                $result = ['$and' => [$result]];
            }

            // Merge the compiled where with the others.
            $compiled = array_merge_recursive($compiled, $result);
        }

        return $compiled;
    }

    /**
     * @param array $where
     * @return array
     */
    protected function compileWhereAll(array $where)
    {
        extract($where);

        return [$column => ['$all' => array_values($values)]];
    }

    /**
     * @param array $where
     * @return array
     */
    protected function compileWhereBasic(array $where)
    {
        extract($where);

        // Replace like with a Regex instance.
        if ($operator == 'like') {
            $operator = '=';

            // Convert to regular expression.
            $regex = preg_replace('#(^|[^\\\])%#', '$1.*', preg_quote($value));

            // Convert like to regular expression.
            if (!Str::startsWith($value, '%')) {
                $regex = '^' . $regex;
            }
            if (!Str::endsWith($value, '%')) {
                $regex = $regex . '$';
            }

            $value = new Regex($regex, 'i');
        } // Manipulate regexp operations.
        elseif (in_array($operator, ['regexp', 'not regexp', 'regex', 'not regex'])) {
            // Automatically convert regular expression strings to Regex objects.
            if (!$value instanceof Regex) {
                $e = explode('/', $value);
                $flag = end($e);
                $regstr = substr($value, 1, -(strlen($flag) + 1));
                $value = new Regex($regstr, $flag);
            }

            // For inverse regexp operations, we can just use the $not operator
            // and pass it a Regex instence.
            if (Str::startsWith($operator, 'not')) {
                $operator = 'not';
            }
        }

        if (!isset($operator) || $operator == '=') {
            $query = [$column => $value];
        } elseif (array_key_exists($operator, $this->conversion)) {
            $query = [$column => [$this->conversion[$operator] => $value]];
        } else {
            $query = [$column => ['$' . $operator => $value]];
        }

        return $query;
    }

    /**
     * @param array $where
     * @return mixed
     */
    protected function compileWhereNested(array $where)
    {
        extract($where);

        return $query->compileWheres();
    }

    /**
     * @param array $where
     * @return array
     */
    protected function compileWhereIn(array $where)
    {
        extract($where);

        return [$column => ['$in' => array_values($values)]];
    }

    /**
     * @param array $where
     * @return array
     */
    protected function compileWhereNotIn(array $where)
    {
        extract($where);

        return [$column => ['$nin' => array_values($values)]];
    }

    /**
     * @param array $where
     * @return array
     */
    protected function compileWhereNull(array $where)
    {
        $where['operator'] = '=';
        $where['value'] = null;

        return $this->compileWhereBasic($where);
    }

    /**
     * @param array $where
     * @return array
     */
    protected function compileWhereNotNull(array $where)
    {
        $where['operator'] = '!=';
        $where['value'] = null;

        return $this->compileWhereBasic($where);
    }

    /**
     * @param array $where
     * @return array
     */
    protected function compileWhereBetween(array $where)
    {
        extract($where);

        if ($not) {
            return [
                '$or' => [
                    [
                        $column => [
                            '$lte' => $values[0],
                        ],
                    ],
                    [
                        $column => [
                            '$gte' => $values[1],
                        ],
                    ],
                ],
            ];
        } else {
            return [
                $column => [
                    '$gte' => $values[0],
                    '$lte' => $values[1],
                ],
            ];
        }
    }

    /**
     * Compile the options to be passed with the query
     *
     * @param  Builder $builder
     * @return array
     */
    protected function compileOptions(Builder $builder): array
    {
        $columns = array_fill_keys($builder->columns ?? [], true);

        if (isset($columns['*'])) {
            $columns = [];
        }

        // Add custom projections.
        if ($builder->projections) {
            $columns = array_merge($columns, $builder->projections);
        }

        $options = [];

        // Apply order, offset, limit and projection
        if ($builder->timeout) {
            $options['maxTimeMS'] = $builder->timeout;
        }

        if ($builder->orders) {
            $options['sort'] = $builder->orders;
        }

        if ($builder->offset) {
            $options['skip'] = $builder->offset;
        }

        if ($builder->limit) {
            $options['limit'] = $builder->limit;
        }

        if ($columns) {
            $options['projection'] = $columns;
        }

        // Fix for legacy support, converts the results to arrays instead of objects.
        $options['typeMap'] = ['root' => 'array', 'document' => 'array'];

        // Add custom query options
        if (count($builder->options)) {
            $options = array_merge($options, $builder->options);
        }

        return $options;
    }

    /**
     * @param array $where
     * @return mixed
     */
    protected function compileWhereRaw(array $where)
    {
        return $where['sql'];
    }
}
