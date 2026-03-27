package geosearch

type btree struct {
	root   *btreeNode
	degree int
}

type btreeNode struct {
	leaf     bool
	keys     []string
	children []*btreeNode //len = len(keys)+1 для внутренних узлов
}

func newBTree(degree int) *btree {
	if degree < 2 {
		degree = 2
	}
	return &btree{
		root:   &btreeNode{leaf: true}, //пустой лись 
		degree: degree,
	}
}

func (t *btree) Insert(key string) {
	if key == "" {
		return
	}

	root := t.root
	// если корень переполнен → сначала делаем split
	// это единственный случай, когда дерево "растет вверх"
	if len(root.keys) == 2*t.degree-1 {
		newRoot := &btreeNode{
			leaf:     false,
			children: []*btreeNode{root},
		}
		t.splitChild(newRoot, 0)
		t.root = newRoot
		t.insertNonFull(newRoot, key)
		return
	}

	t.insertNonFull(root, key)
}

func (t *btree) Contains(key string) bool {
	return containsNode(t.root, key)
}

func (t *btree) Keys() []string { //все ключи (in-order обход)
	keys := make([]string, 0)
	collectKeys(t.root, &keys)
	return keys
}

func containsNode(node *btreeNode, key string) bool {
	i := 0
	for i < len(node.keys) && key > node.keys[i] { //идем вправо 
		i++
	}

	if i < len(node.keys) && key == node.keys[i] { //точное совпадение
		return true
	}

	if node.leaf { //// если лист → дальше идти некуда
		return false
	}

	return containsNode(node.children[i], key)
}

func collectKeys(node *btreeNode, keys *[]string) { //(in-order traversal)
	if node == nil {
		return
	}

	for i := range node.keys {
		if !node.leaf {
			collectKeys(node.children[i], keys)
		}
		*keys = append(*keys, node.keys[i])
	}

	if !node.leaf {
		collectKeys(node.children[len(node.keys)], keys)
	}
}

func (t *btree) splitChild(parent *btreeNode, childIndex int) {
	fullChild := parent.children[childIndex]
	newSibling := &btreeNode{leaf: fullChild.leaf} // создаем нового соседа (правую часть)
	mid := t.degree - 1

	// cредний ключ поднимается в родител а левый и правый куски остаются в двух отдельных детях.
	parent.keys = append(parent.keys, "")
	copy(parent.keys[childIndex+1:], parent.keys[childIndex:])
	parent.keys[childIndex] = fullChild.keys[mid]

	parent.children = append(parent.children, nil)
	copy(parent.children[childIndex+2:], parent.children[childIndex+1:])
	parent.children[childIndex+1] = newSibling

	newSibling.keys = append(newSibling.keys, fullChild.keys[mid+1:]...)
	fullChild.keys = fullChild.keys[:mid]

	if !fullChild.leaf {
		newSibling.children = append(newSibling.children, fullChild.children[mid+1:]...)
		fullChild.children = fullChild.children[:mid+1]
	}
}

// вставка в узел, который гарантированно НЕ переполнен
func (t *btree) insertNonFull(node *btreeNode, key string) {
	i := len(node.keys) - 1

	if node.leaf { // вставка в лист
		node.keys = append(node.keys, "")

		for i >= 0 && key < node.keys[i] {
			node.keys[i+1] = node.keys[i]
			i--
		}

		if i >= 0 && node.keys[i] == key {
			// дубликаты храним не в дереве, а в bucket map
			node.keys = node.keys[:len(node.keys)-1]
			return
		}

		node.keys[i+1] = key
		return
	}

	// ищем ребенка, куда спускаться
	for i >= 0 && key < node.keys[i] {
		i--
	}
	i++

	// если ключ уже есть 
	if i-1 >= 0 && node.keys[i-1] == key {
		return
	}

	// если ребенок переполнен → делим его
	if len(node.children[i].keys) == 2*t.degree-1 {
		t.splitChild(node, i)

		switch {
		case key == node.keys[i]:
			return
		case key > node.keys[i]:
			i++
		}
	}

	// рекурсивно вставляем вниз
	t.insertNonFull(node.children[i], key)
}