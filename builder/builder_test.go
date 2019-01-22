package builder

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildHaving(t *testing.T) {
	type inStruct struct {
		table       string
		where       map[string]interface{}
		selectField []string
	}
	type outStruct struct {
		cond string
		vals []interface{}
		err  error
	}
	var data = []struct {
		in  inStruct
		out outStruct
	}{
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"age > ": 23,
				},
				selectField: []string{"count(*) as total"},
			},
			out: outStruct{
				cond: "SELECT count(*) as total FROM tb WHERE (age>$1)",
				vals: []interface{}{23},
				err:  nil,
			},
		},
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"age > ":   23,
					"_groupby": "name",
					"_having": map[string]interface{}{
						"total >=": 1000,
						"total <":  50000,
					},
				},
				selectField: []string{"name, count(price) as total"},
			},
			out: outStruct{
				cond: "SELECT name, count(price) as total FROM tb WHERE (age>$1) GROUP BY name HAVING (total>=$2 AND total<$3)",
				vals: []interface{}{23, 1000, 50000},
				err:  nil,
			},
		},
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"_groupby": "name",
					"_having": map[string]interface{}{
						"total >=": 1000,
						"total <":  50000,
					},
				},
				selectField: []string{"name, count(price) as total"},
			},
			out: outStruct{
				cond: "SELECT name, count(price) as total FROM tb GROUP BY name HAVING (total>=$1 AND total<$2)",
				vals: []interface{}{1000, 50000},
				err:  nil,
			},
		},
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"_having": map[string]interface{}{
						"total >=": 1000,
						"total <":  50000,
					},
					"age in": []interface{}{1, 2, 3},
				},
				selectField: []string{"name, age"},
			},
			out: outStruct{
				cond: "SELECT name, age FROM tb WHERE (age IN ($1,$2,$3))",
				vals: []interface{}{1, 2, 3},
				err:  nil,
			},
		},
	}
	ass := assert.New(t)
	for _, tc := range data {
		cond, vals, err := BuildSelect(tc.in.table, tc.in.where, tc.in.selectField)
		ass.Equal(tc.out.err, err)
		ass.Equal(tc.out.cond, cond)
		ass.Equal(tc.out.vals, vals)
	}
}

func Test_BuildInsert(t *testing.T) {
	ass := assert.New(t)
	type inStruct struct {
		table   string
		setData []map[string]interface{}
	}
	type outStruct struct {
		cond string
		vals []interface{}
		err  error
	}
	var data = []struct {
		in  inStruct
		out outStruct
	}{
		{
			in: inStruct{
				table: "tb",
				setData: []map[string]interface{}{
					{
						"foo": "bar",
						"age": 23,
					},
				},
			},
			out: outStruct{
				cond: "INSERT INTO tb (age,foo) VALUES ($1,$2)",
				vals: []interface{}{23, "bar"},
				err:  nil,
			},
		},
	}
	for _, tc := range data {
		cond, vals, err := BuildInsert(tc.in.table, tc.in.setData)
		ass.Equal(tc.out.err, err)
		ass.Equal(tc.out.cond, cond)
		ass.Equal(tc.out.vals, vals)
	}
}

func Test_BuildDelete(t *testing.T) {
	ass := assert.New(t)
	type inStruct struct {
		table string
		where map[string]interface{}
	}
	type outStruct struct {
		cond string
		vals []interface{}
		err  error
	}
	var data = []struct {
		in  inStruct
		out outStruct
	}{
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"age >=":   21,
					"sex in":   []interface{}{"male", "female"},
					"hobby in": []interface{}{"soccer", "basketball", "tenis"},
				},
			},
			out: outStruct{
				cond: "DELETE FROM tb WHERE (hobby IN ($1,$2,$3) AND sex IN ($4,$5) AND age>=$6)",
				vals: []interface{}{"soccer", "basketball", "tenis", "male", "female", 21},
				err:  nil,
			},
		},
	}
	for _, tc := range data {
		cond, vals, err := BuildDelete(tc.in.table, tc.in.where)
		ass.Equal(tc.out.err, err)
		ass.Equal(tc.out.cond, cond)
		ass.Equal(tc.out.vals, vals)
	}
}

func Test_BuildUpdate(t *testing.T) {
	type inStruct struct {
		table   string
		where   map[string]interface{}
		setData map[string]interface{}
	}
	type outStruct struct {
		cond string
		vals []interface{}
		err  error
	}
	var data = []struct {
		in  inStruct
		out outStruct
	}{
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"foo":    "bar",
					"age >=": 23,
					"sex in": []interface{}{"male", "female"},
				},
				setData: map[string]interface{}{
					"score":    50,
					"district": "010",
				},
			},
			out: outStruct{
				cond: "UPDATE tb SET district=$1,score=$2 WHERE (foo=$3 AND sex IN ($4,$5) AND age>=$6)",
				vals: []interface{}{"010", 50, "bar", "male", "female", 23},
				err:  nil,
			},
		},
	}
	ass := assert.New(t)
	for _, tc := range data {
		cond, vals, err := BuildUpdate(tc.in.table, tc.in.where, tc.in.setData)
		ass.Equal(tc.out.err, err)
		ass.Equal(tc.out.cond, cond)
		ass.Equal(tc.out.vals, vals)
	}
}

func Test_BuildSelect(t *testing.T) {
	type inStruct struct {
		table  string
		where  map[string]interface{}
		fields []string
	}
	type outStruct struct {
		cond string
		vals []interface{}
		err  error
	}
	var data = []struct {
		in  inStruct
		out outStruct
	}{
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"foo":      "bar",
					"qq":       "tt",
					"age in":   []interface{}{1, 3, 5, 7, 9},
					"faith <>": "Muslim",
					"_orderby": "age desc",
					"_groupby": "department",
					"_limit":   []uint{0, 100},
				},
				fields: []string{"id", "name", "age"},
			},
			out: outStruct{
				cond: "SELECT id,name,age FROM tb WHERE (foo=$1 AND qq=$2 AND age IN ($3,$4,$5,$6,$7) AND faith!=$8) GROUP BY department ORDER BY age DESC LIMIT 0 OFFSET 100",
				vals: []interface{}{"bar", "tt", 1, 3, 5, 7, 9, "Muslim"},
				err:  nil,
			},
		},
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"name like": "%123",
				},
				fields: nil,
			},
			out: outStruct{
				cond: `SELECT * FROM tb WHERE (name LIKE $1)`,
				vals: []interface{}{"%123"},
				err:  nil,
			},
		},
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"name": "caibirdme",
				},
				fields: nil,
			},
			out: outStruct{
				cond: "SELECT * FROM tb WHERE (name=$1)",
				vals: []interface{}{"caibirdme"},
				err:  nil,
			},
		},
	}
	ass := assert.New(t)
	for _, tc := range data {
		cond, vals, err := BuildSelect(tc.in.table, tc.in.where, tc.in.fields)
		ass.Equal(tc.out.err, err)
		ass.Equal(tc.out.cond, cond)
		ass.Equal(tc.out.vals, vals)
	}
}

func BenchmarkBuildSelect_Sequelization(b *testing.B) {
	for i := 0; i < b.N; i++ {
		BuildSelect("tb", map[string]interface{}{
			"foo":      "bar",
			"qq":       "tt",
			"age in":   []interface{}{1, 3, 5, 7, 9},
			"faith <>": "Muslim",
			"_orderby": "age desc",
			"_groupby": "department",
			"_limit":   []uint{0, 100},
		}, []string{"a", "b", "c"})
	}
}

func BenchmarkBuildSelect_Parallel(b *testing.B) {
	expectCond := "SELECT * FROM tb WHERE (foo=$1 AND qq=$2 AND age IN ($3,$4,$5,$6,$7) AND faith!=$8) GROUP BY department ORDER BY age DESC LIMIT 0 OFFSET 100"
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cond, _, _ := BuildSelect("tb", map[string]interface{}{
				"foo":      "bar",
				"qq":       "tt",
				"age in":   []interface{}{1, 3, 5, 7, 9},
				"faith <>": "Muslim",
				"_orderby": "age desc",
				"_groupby": "department",
				"_limit":   []uint{0, 100},
			}, nil)
			if cond != expectCond {
				b.Errorf("should be %s but %s\n", expectCond, cond)
			}
		}
	})
}

func TestLike(t *testing.T) {
	type inStruct struct {
		table  string
		where  map[string]interface{}
		fields []string
	}
	type outStruct struct {
		cond string
		vals []interface{}
		err  error
	}
	var data = []struct {
		in  inStruct
		out outStruct
	}{
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"bar like": "haha%",
					"baz like": "%some",
					"foo":      1,
				},
				fields: nil,
			},
			out: outStruct{
				cond: `SELECT * FROM tb WHERE (foo=$1 AND bar LIKE $2 AND baz LIKE $3)`,
				vals: []interface{}{1, "haha%", "%some"},
				err:  nil,
			},
		},
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"bar like": "haha%",
					"baz like": "%some",
					"foo":      1,
					"age in":   []interface{}{1, 3, 5, 7, 9},
				},
				fields: nil,
			},
			out: outStruct{
				cond: `SELECT * FROM tb WHERE (foo=$1 AND age IN ($2,$3,$4,$5,$6) AND bar LIKE $7 AND baz LIKE $8)`,
				vals: []interface{}{1, 1, 3, 5, 7, 9, "haha%", "%some"},
				err:  nil,
			},
		},
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"name like": "%James",
				},
				fields: []string{"name"},
			},
			out: outStruct{
				cond: `SELECT name FROM tb WHERE (name LIKE $1)`,
				vals: []interface{}{"%James"},
				err:  nil,
			},
		},
	}
	ass := assert.New(t)
	for _, tc := range data {
		cond, vals, err := BuildSelect(tc.in.table, tc.in.where, tc.in.fields)
		ass.Equal(tc.out.err, err)
		ass.Equal(tc.out.cond, cond)
		ass.Equal(tc.out.vals, vals)
	}
}

func TestNamedQuery(t *testing.T) {
	var testData = []struct {
		sql  string
		data map[string]interface{}
		cond string
		vals []interface{}
		err  error
	}{
		{
			sql: `select * from tb where name={{name}}`,
			data: map[string]interface{}{
				"age": 24,
			},
			cond: "",
			vals: nil,
			err:  errors.New("name not found"),
		},
		{
			sql:  `select * from tb where name=hello`,
			data: nil,
			cond: "select * from tb where name=hello",
			vals: nil,
			err:  nil,
		},
		{
			sql: `select * from tb where name={{name}} and age<{{age}}`,
			data: map[string]interface{}{
				"age": 24,
			},
			cond: "",
			vals: nil,
			err:  errors.New("name not found"),
		},
		{
			sql: `select * from tb where name={{name}} and age<>{{age}}`,
			data: map[string]interface{}{
				"name": "caibirdme",
				"age":  24,
			},
			cond: `select * from tb where name=$1 and age<>$2`,
			vals: []interface{}{"caibirdme", 24},
			err:  nil,
		},
		{
			sql: `select * from tb where name={{name}} and age in {{age}}`,
			data: map[string]interface{}{
				"name": "caibirdme",
				"age":  []int{1, 2, 3},
			},
			cond: `select * from tb where name=$1 and age in ($2,$3,$4)`,
			vals: []interface{}{"caibirdme", 1, 2, 3},
			err:  nil,
		},
		{
			sql: `select * from tb where name={{name}} and age in (select m_age from anothertb where m_age>{{m_age}})`,
			data: map[string]interface{}{
				"name":  "caibirdme",
				"m_age": 88.9,
			},
			cond: `select * from tb where name=$1 and age in (select m_age from anothertb where m_age>$2)`,
			vals: []interface{}{"caibirdme", 88.9},
			err:  nil,
		},
		{
			sql: `select * from tb where age in {{some}} and other in {{some}}`,
			data: map[string]interface{}{
				"some": []float64{24.0, 28.7},
			},
			cond: "select * from tb where age in ($1,$2) and other in ($3,$4)",
			vals: []interface{}{24.0, 28.7, 24.0, 28.7},
			err:  nil,
		},
		{
			sql: `select a.name,a.age from tb1 as a join tb2 as b on a.id=b.id where a.age>{{age}} and b.age<{{foo}} order by a.name desc limit {{limit}}`,
			data: map[string]interface{}{
				"age":   20,
				"foo":   30,
				"limit": 40,
			},
			cond: "select a.name,a.age from tb1 as a join tb2 as b on a.id=b.id where a.age>$1 and b.age<$2 order by a.name desc limit $3",
			vals: []interface{}{20, 30, 40},
			err:  nil,
		},
		{
			sql: `select * from tb where age in {{age}}`,
			data: map[string]interface{}{
				"age": []int{1},
			},
			cond: `select * from tb where age in ($1)`,
			vals: []interface{}{1},
			err:  nil,
		},
	}
	ass := assert.New(t)
	for _, tc := range testData {
		cond, vals, err := NamedQuery(tc.sql, tc.data)
		if !ass.Equal(tc.err, err) {
			return
		}
		ass.Equal(tc.cond, cond)
		ass.Equal(tc.vals, vals)
	}
}

func Test_BuildIN(t *testing.T) {
	type inStruct struct {
		table  string
		where  map[string]interface{}
		fields []string
	}
	type outStruct struct {
		cond string
		vals []interface{}
		err  error
	}
	var data = []struct {
		in  inStruct
		out outStruct
	}{
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"foo":      "bar",
					"qq":       "tt",
					"age in":   []int{1, 3, 5, 7, 9},
					"faith <>": "Muslim",
					"_orderby": "age desc",
					"_groupby": "department",
				},
				fields: []string{"id", "name", "age"},
			},
			out: outStruct{
				cond: "SELECT id,name,age FROM tb WHERE (foo=$1 AND qq=$2 AND age IN ($3,$4,$5,$6,$7) AND faith!=$8) GROUP BY department ORDER BY age DESC",
				vals: []interface{}{"bar", "tt", 1, 3, 5, 7, 9, "Muslim"},
				err:  nil,
			},
		},
	}
	ass := assert.New(t)
	for _, tc := range data {
		cond, vals, err := BuildSelect(tc.in.table, tc.in.where, tc.in.fields)
		ass.Equal(tc.out.err, err)
		ass.Equal(tc.out.cond, cond)
		ass.Equal(tc.out.vals, vals)
	}
}

func Benchmark_BuildIN(b *testing.B) {
	where := map[string]interface{}{
		"age": []uint64{1, 3, 5, 7, 9},
	}
	for i := 0; i < b.N; i++ {
		convertWhereMapToWhereMapSlice(where)
	}
}

func Test_BuildOrderBy(t *testing.T) {
	type inStruct struct {
		table  string
		where  map[string]interface{}
		fields []string
	}
	type outStruct struct {
		cond string
		vals []interface{}
		err  error
	}
	var data = []struct {
		in  inStruct
		out outStruct
	}{
		{
			in: inStruct{
				table: "tb",
				where: map[string]interface{}{
					"foo":      "bar",
					"_orderby": "age desc, id asc",
				},
				fields: []string{"id", "name", "age"},
			},
			out: outStruct{
				cond: "SELECT id,name,age FROM tb WHERE (foo=$1) ORDER BY age DESC,id ASC",
				vals: []interface{}{"bar"},
				err:  nil,
			},
		},
	}
	ass := assert.New(t)
	for _, tc := range data {
		cond, vals, err := BuildSelect(tc.in.table, tc.in.where, tc.in.fields)
		ass.Equal(tc.out.err, err)
		ass.Equal(tc.out.cond, cond)
		ass.Equal(tc.out.vals, vals)
	}
}
