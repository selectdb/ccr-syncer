package base

type ISpecFactory interface {
	NewISpec(tableSpec *Spec) Specer
}

type SpecFactory struct {
}

func NewISpecFactory() ISpecFactory {
	return &SpecFactory{}
}

func (sf *SpecFactory) NewISpec(spec *Spec) Specer {
	return spec
}
