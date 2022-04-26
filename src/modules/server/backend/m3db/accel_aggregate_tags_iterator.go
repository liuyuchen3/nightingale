package m3db

import (
	"bytes"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/x/ident"
)

// 为了加速从M3DB的Tag查询封装的聚合tag的迭代器
// Author: liuyuchen12@tal.com
// Date: 2022/4/26
type accelAggregateTagsIterator struct {
	currentIdx int
	err        error

	current struct {
		tagName   ident.ID
		tagValues ident.Iterator
	}

	backing []*accelAggregateTagsIteratorTag
	backingMap map[string]*accelAggregateTagsIteratorTag
}

type accelAggregateTagsIteratorTag struct {
	tagName   ident.ID
	tagValues []ident.ID
}

// make the compiler ensure the concrete type `&accelAggregateTagsIterator{}` implements
// the `AggregatedTagsIterator` interface.
var _ client.AggregatedTagsIterator = &accelAggregateTagsIterator{}

func newAggregateTagsIterator() *accelAggregateTagsIterator {
	return &accelAggregateTagsIterator{
		currentIdx: -1,
		backingMap: make(map[string]*accelAggregateTagsIteratorTag),
	}
}

func (i *accelAggregateTagsIterator) release() {
	i.current.tagName = nil
	i.current.tagValues = nil
}

func (i *accelAggregateTagsIterator) Next() bool {
	if i.err != nil || i.currentIdx >= len(i.backing) {
		return false
	}

	i.release()
	i.currentIdx++
	if i.currentIdx >= len(i.backing) {
		return false
	}

	i.current.tagName = i.backing[i.currentIdx].tagName
	i.current.tagValues = ident.NewIDSliceIterator(i.backing[i.currentIdx].tagValues)
	return true
}

func (i *accelAggregateTagsIterator) addTag(tag ident.Tag) {
	// get tag or create new one
	tagWithValues, ok := i.backingMap[tag.Name.String()]
	if !ok {
		tagWithValues = 	&accelAggregateTagsIteratorTag{
			tagName: ident.BytesID(tag.Name.String()),
		}
		i.backing = append(i.backing, tagWithValues)
		i.backingMap[tag.Name.String()] = tagWithValues
	}

	if len(tagWithValues.tagValues) == 0 {
		// If first response with values from host then add in order blindly.
		tagWithValues.tagValues = append(tagWithValues.tagValues, ident.BytesID(tag.Value.Bytes()))
		return
	}

	// 截止到此方法结束的代码都是为了插入Tag Value的时候保持一个有序的状态
	var tempValues []ident.ID = make([]ident.ID, 0, len(tagWithValues.tagValues))
	tempValues = tempValues[:0]

	lastValueIdx := 0
	addRemaining := false
	nextLastValue := func() {
		lastValueIdx++
		if lastValueIdx >= len(tagWithValues.tagValues) {
			// None left to compare against, just blindly add the remaining.
			addRemaining = true
		}
	}

	currValue := ident.BytesID(tag.Value.Bytes())
	existingValue := tagWithValues.tagValues[lastValueIdx]
	cmp := bytes.Compare(currValue.Bytes(), existingValue.Bytes())
	for cmp > 0 {
		// Take the existing value
		tempValues = append(tempValues, existingValue)

		// Move to next record
		nextLastValue()
		if addRemaining {
			// None left to compare against, just blindly add the remaining.
			tempValues = append(tempValues, currValue)
			tagWithValues.tagValues = append(tagWithValues.tagValues[:0], tempValues...)
			return
		}

		// Re-run check
		existingValue = tagWithValues.tagValues[lastValueIdx]
		cmp = bytes.Compare(currValue.Bytes(), existingValue.Bytes())
	}

	if cmp == 0 {
		// Take existing record, skip this copy
		tempValues = append(tempValues, existingValue)
	} else {
		tempValues = append(tempValues, currValue)
		tempValues = append(tempValues, existingValue)
	}

	// Take existing record left
	nextLastValue()
	for !addRemaining {
		tempValues = append(tempValues, tagWithValues.tagValues[lastValueIdx])
		nextLastValue()
	}

	// Copy out of temp values back to final result
	tagWithValues.tagValues = append(tagWithValues.tagValues[:0], tempValues...)
}



func (i *accelAggregateTagsIterator) Finalize() {
	i.release()
	for _, b := range i.backing {
		b.tagName.Finalize()
		for _, v := range b.tagValues {
			v.Finalize()
		}
	}
	i.backing = nil
}

func (i *accelAggregateTagsIterator) Remaining() int {
	return len(i.backing) - (i.currentIdx + 1)
}

func (i *accelAggregateTagsIterator) Current() (tagName ident.ID, tagValues ident.Iterator) {
	return i.current.tagName, i.current.tagValues
}

func (i *accelAggregateTagsIterator) Err() error {
	return i.err
}

