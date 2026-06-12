package query

import "hw5/internal/index"

type Iterator = index.PostingIterator

func NewIterator(pl index.PostingList) *Iterator {
	return index.NewPostingIterator(pl)
}
