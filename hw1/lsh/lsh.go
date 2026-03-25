package lsh

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
)

var (
	ErrInvalidConfig  = errors.New("invalid lsh config")
	ErrEmptyPointID   = errors.New("point id must not be empty")
	ErrDuplicatePoint = errors.New("duplicate point id")
)

type Point struct {
	ID string
	X  float64
	Y  float64
	Z  float64
}

type Match struct {
	ID       string
	Distance float64
}

type Config struct {
	Tables   int
	CellSize float64
	Radius   float64
	Seed     int64
}

type Stats struct {
	PointsCount   int
	Tables        int
	BucketsCount  int
	MaxBucketLoad int
	AvgBucketLoad float64
}

type cell struct {
	X int
	Y int
	Z int
}

type gridTable struct {
	ShiftX  float64
	ShiftY  float64
	ShiftZ  float64
	Buckets map[cell][]int
}

type Index struct {
	cfg    Config
	points []Point
	byID   map[string]int
	tables []gridTable
}

func Build(points []Point, cfg Config) (*Index, error) {
	index, err := newIndex(cfg)
	if err != nil {
		return nil, err
	}

	for _, point := range points {
		if err := index.Add(point); err != nil {
			return nil, err
		}
	}

	return index, nil
}

func newIndex(cfg Config) (*Index, error) {
	if cfg.Tables <= 0 || cfg.CellSize <= 0 || cfg.Radius < 0 || cfg.Radius > cfg.CellSize {
		return nil, ErrInvalidConfig
	}

	rng := rand.New(rand.NewSource(cfg.Seed))
	tables := make([]gridTable, cfg.Tables)
	for i := range tables {
		tables[i] = gridTable{
			ShiftX:  rng.Float64() * cfg.CellSize,
			ShiftY:  rng.Float64() * cfg.CellSize,
			ShiftZ:  rng.Float64() * cfg.CellSize,
			Buckets: make(map[cell][]int),
		}
	}

	return &Index{
		cfg:    cfg,
		points: make([]Point, 0),
		byID:   make(map[string]int),
		tables: tables,
	}, nil
}

func (i *Index) Add(point Point) error {
	if point.ID == "" {
		return ErrEmptyPointID
	}
	if _, exists := i.byID[point.ID]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicatePoint, point.ID)
	}

	index := len(i.points)
	i.points = append(i.points, point)
	i.byID[point.ID] = index

	for tableIndex := range i.tables {
		key := i.cellFor(tableIndex, point)
		i.tables[tableIndex].Buckets[key] = append(i.tables[tableIndex].Buckets[key], index)
	}

	return nil
}

func (i *Index) Search(query Point) []Match {
	candidates := make(map[int]struct{})

	for tableIndex := range i.tables {
		base := i.cellFor(tableIndex, query)
		for dx := -1; dx <= 1; dx++ {
			for dy := -1; dy <= 1; dy++ {
				for dz := -1; dz <= 1; dz++ {
					neighbor := cell{X: base.X + dx, Y: base.Y + dy, Z: base.Z + dz}
					for _, pointIndex := range i.tables[tableIndex].Buckets[neighbor] {
						candidates[pointIndex] = struct{}{}
					}
				}
			}
		}
	}

	return i.collectMatches(query, candidates)
}

func (i *Index) FullScan(query Point) []Match {
	candidates := make(map[int]struct{}, len(i.points))
	for pointIndex := range i.points {
		candidates[pointIndex] = struct{}{}
	}

	return i.collectMatches(query, candidates)
}

func (i *Index) Stats() Stats {
	stats := Stats{
		PointsCount: len(i.points),
		Tables:      len(i.tables),
	}

	totalLoads := 0
	for _, table := range i.tables {
		stats.BucketsCount += len(table.Buckets)
		for _, bucket := range table.Buckets {
			load := len(bucket)
			totalLoads += load
			if load > stats.MaxBucketLoad {
				stats.MaxBucketLoad = load
			}
		}
	}

	if stats.BucketsCount > 0 {
		stats.AvgBucketLoad = float64(totalLoads) / float64(stats.BucketsCount)
	}

	return stats
}

func (i *Index) collectMatches(query Point, candidates map[int]struct{}) []Match {
	matches := make([]Match, 0)

	for pointIndex := range candidates {
		point := i.points[pointIndex]
		if query.ID != "" && point.ID == query.ID {
			continue
		}

		distance := pointDistance(query, point)
		if distance <= i.cfg.Radius {
			matches = append(matches, Match{ID: point.ID, Distance: distance})
		}
	}

	sort.Slice(matches, func(a, b int) bool {
		if matches[a].Distance == matches[b].Distance {
			return matches[a].ID < matches[b].ID
		}
		return matches[a].Distance < matches[b].Distance
	})

	return matches
}

func (i *Index) cellFor(tableIndex int, point Point) cell {
	table := i.tables[tableIndex]
	return cell{
		X: int(math.Floor((point.X + table.ShiftX) / i.cfg.CellSize)),
		Y: int(math.Floor((point.Y + table.ShiftY) / i.cfg.CellSize)),
		Z: int(math.Floor((point.Z + table.ShiftZ) / i.cfg.CellSize)),
	}
}

func pointDistance(a Point, b Point) float64 {
	dx := a.X - b.X
	dy := a.Y - b.Y
	dz := a.Z - b.Z
	return math.Sqrt(dx*dx + dy*dy + dz*dz)
}
