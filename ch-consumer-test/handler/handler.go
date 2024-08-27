package handler

import (
	"context"
)

type Handler interface {
	HandleObject(ctx context.Context, object any) error
	onCreate(ctx context.Context, object any) error
	onUpdate(ctx context.Context, object any) error
	onDelete(ctx context.Context, object any) error
}

type handler struct{}

func NewHandler() Handler {
	return &handler{}
}

func (h *handler) HandleObject(ctx context.Context, object any) error {
	return nil
}

func (h *handler) onCreate(ctx context.Context, object any) error {
	return nil
}

func (h *handler) onUpdate(ctx context.Context, object any) error {
	return nil
}

func (h *handler) onDelete(ctx context.Context, object any) error {
	return nil
}
