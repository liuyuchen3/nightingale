package models

import (
	"errors"
	"sort"
	"strings"
	"time"
)

// MetricView 在告警聚合视图查看的时候，要存储一些聚合规则
type MetricView struct {
	Id       int64  `json:"id" gorm:"primaryKey"`
	Name     string `json:"name"`
	Cate     int    `json:"cate"`
	Configs  string `json:"configs"`
	CreateAt int64  `json:"create_at"`
	CreateBy int64  `json:"create_by"`
	UpdateAt int64  `json:"update_at"`
}

func (v *MetricView) TableName() string {
	return "metric_view"
}

func (v *MetricView) Verify() error {
	v.Name = strings.TrimSpace(v.Name)
	if v.Name == "" {
		return errors.New("name is blank")
	}

	v.Configs = strings.TrimSpace(v.Configs)
	if v.Configs == "" {
		return errors.New("configs is blank")
	}

	return nil
}

func (v *MetricView) Add() error {
	if err := v.Verify(); err != nil {
		return err
	}

	now := time.Now().Unix()
	v.CreateAt = now
	v.UpdateAt = now
	v.Cate = 1
	return Insert(v)
}

func (v *MetricView) Update(name, configs string) error {
	if err := v.Verify(); err != nil {
		return err
	}

	v.UpdateAt = time.Now().Unix()
	v.Name = name
	v.Configs = configs

	return DB().Model(v).Select("name", "configs", "update_at").Updates(v).Error
}

// MetricViewDel: userid for safe delete
func MetricViewDel(ids []int64, createBy interface{}) error {
	if len(ids) == 0 {
		return nil
	}

	return DB().Where("id in ? and create_by = ?", ids, createBy).Delete(new(MetricView)).Error
}

func MetricViewGets(createBy interface{}) ([]MetricView, error) {
	var lst []MetricView
	err := DB().Where("create_by = ? or cate = 0", createBy).Find(&lst).Error
	if err == nil && len(lst) > 1 {
		sort.Slice(lst, func(i, j int) bool {
			if lst[i].Cate < lst[j].Cate {
				return true
			}

			if lst[i].Cate > lst[j].Cate {
				return false
			}

			return lst[i].Name < lst[j].Name
		})
	}
	return lst, err
}

func MetricViewGet(where string, args ...interface{}) (*MetricView, error) {
	var lst []*MetricView
	err := DB().Where(where, args...).Find(&lst).Error
	if err != nil {
		return nil, err
	}

	if len(lst) == 0 {
		return nil, nil
	}

	return lst[0], nil
}
