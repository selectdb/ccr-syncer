package base

type ISpecFactory interface {
	NewISpec(tableSpec *Spec) ISpec
}

type SpecFactory struct {
}

func NewISpecFactory() ISpecFactory {
	return &SpecFactory{}
}

func (sf *SpecFactory) NewISpec(spec *Spec) ISpec {
	return spec
}
