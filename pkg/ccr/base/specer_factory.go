package base

type SpecerFactory interface {
	NewSpecer(tableSpec *Spec) Specer
}

type SpecFactory struct{}

func NewSpecerFactory() SpecerFactory {
	return &SpecFactory{}
}

func (sf *SpecFactory) NewSpecer(spec *Spec) Specer {
	return spec
}
