package utils

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBoolExprHelpers(t *testing.T) {
	t.Run("AlwaysTrue", func(t *testing.T) {
		ctx := context.Background()
		expr := AlwaysTrue[string]()

		result, err := expr.Eval(ctx, "test")
		require.NoError(t, err)
		assert.True(t, result)

		result, err = expr.Eval(ctx, "")
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("Not with AlwaysTrue", func(t *testing.T) {
		ctx := context.Background()
		expr := Not(AlwaysTrue[string]())

		result, err := expr.Eval(ctx, "test")
		require.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("Not with custom matcher", func(t *testing.T) {
		ctx := context.Background()

		// Create a matcher that returns true for non-empty strings
		nonEmptyMatcher := &mockBoolExpr[string]{
			evalFunc: func(ctx context.Context, val string) (bool, error) {
				return len(val) > 0, nil
			},
		}

		notExpr := Not[string](nonEmptyMatcher)

		result, err := notExpr.Eval(ctx, "test")
		require.NoError(t, err)
		assert.False(t, result, "Should be false for non-empty string")

		result, err = notExpr.Eval(ctx, "")
		require.NoError(t, err)
		assert.True(t, result, "Should be true for empty string")
	})

	t.Run("Or with no matchers", func(t *testing.T) {
		expr := Or[string]()
		assert.Nil(t, expr)
	})

	t.Run("Or with single matcher", func(t *testing.T) {
		ctx := context.Background()
		matcher := AlwaysTrue[string]()
		expr := Or(matcher)

		// Or with single matcher should return that matcher (checking by evaluation)
		assert.NotNil(t, expr)

		result, err := expr.Eval(ctx, "test")
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("Or with multiple matchers - first true", func(t *testing.T) {
		ctx := context.Background()

		trueExpr := &mockBoolExpr[string]{
			evalFunc: func(ctx context.Context, val string) (bool, error) {
				return true, nil
			},
		}

		falseExpr := &mockBoolExpr[string]{
			evalFunc: func(ctx context.Context, val string) (bool, error) {
				return false, nil
			},
		}

		expr := Or(trueExpr, falseExpr)

		result, err := expr.Eval(ctx, "test")
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("Or with multiple matchers - all false", func(t *testing.T) {
		ctx := context.Background()

		falseExpr1 := &mockBoolExpr[string]{
			evalFunc: func(ctx context.Context, val string) (bool, error) {
				return false, nil
			},
		}

		falseExpr2 := &mockBoolExpr[string]{
			evalFunc: func(ctx context.Context, val string) (bool, error) {
				return false, nil
			},
		}

		expr := Or(falseExpr1, falseExpr2)

		result, err := expr.Eval(ctx, "test")
		require.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("Or with multiple matchers - second true", func(t *testing.T) {
		ctx := context.Background()

		falseExpr := &mockBoolExpr[string]{
			evalFunc: func(ctx context.Context, val string) (bool, error) {
				return false, nil
			},
		}

		trueExpr := &mockBoolExpr[string]{
			evalFunc: func(ctx context.Context, val string) (bool, error) {
				return true, nil
			},
		}

		expr := Or(falseExpr, trueExpr)

		result, err := expr.Eval(ctx, "test")
		require.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("Or short-circuits on first true", func(t *testing.T) {
		ctx := context.Background()
		var secondEvaluated bool

		trueExpr := &mockBoolExpr[string]{
			evalFunc: func(ctx context.Context, val string) (bool, error) {
				return true, nil
			},
		}

		trackingExpr := &mockBoolExpr[string]{
			evalFunc: func(ctx context.Context, val string) (bool, error) {
				secondEvaluated = true
				return false, nil
			},
		}

		expr := Or(trueExpr, trackingExpr)

		result, err := expr.Eval(ctx, "test")
		require.NoError(t, err)
		assert.True(t, result)
		assert.False(t, secondEvaluated, "Second matcher should not be evaluated")
	})

	t.Run("Or error propagation", func(t *testing.T) {
		ctx := context.Background()
		expectedErr := assert.AnError

		errorExpr := &mockBoolExpr[string]{
			evalFunc: func(ctx context.Context, val string) (bool, error) {
				return false, expectedErr
			},
		}

		expr := Or(errorExpr)

		result, err := expr.Eval(ctx, "test")
		assert.False(t, result)
		assert.ErrorIs(t, err, expectedErr)
	})
}

func TestComplexExpressions(t *testing.T) {
	t.Run("Not(Or(...))", func(t *testing.T) {
		ctx := context.Background()

		alwaysFalse := &mockBoolExpr[int]{
			evalFunc: func(ctx context.Context, val int) (bool, error) {
				return false, nil
			},
		}

		expr := Not(Or(alwaysFalse, alwaysFalse))

		result, err := expr.Eval(ctx, 42)
		require.NoError(t, err)
		assert.True(t, result, "Not of Or(false, false) should be true")
	})

	t.Run("Or(Not(...), Not(...))", func(t *testing.T) {
		ctx := context.Background()

		alwaysTrue := &mockBoolExpr[int]{
			evalFunc: func(ctx context.Context, val int) (bool, error) {
				return true, nil
			},
		}

		alwaysFalse := &mockBoolExpr[int]{
			evalFunc: func(ctx context.Context, val int) (bool, error) {
				return false, nil
			},
		}

		expr := Or(Not[int](alwaysTrue), Not[int](alwaysFalse))

		result, err := expr.Eval(ctx, 42)
		require.NoError(t, err)
		assert.True(t, result, "Or(Not(true), Not(false)) should be true")
	})

	t.Run("nested Or expressions", func(t *testing.T) {
		ctx := context.Background()

		isPositive := &mockBoolExpr[int]{
			evalFunc: func(ctx context.Context, val int) (bool, error) {
				return val > 0, nil
			},
		}

		isEven := &mockBoolExpr[int]{
			evalFunc: func(ctx context.Context, val int) (bool, error) {
				return val%2 == 0, nil
			},
		}

		// (positive OR even)
		expr := Or(isPositive, isEven)

		result, err := expr.Eval(ctx, 3)
		require.NoError(t, err)
		assert.True(t, result, "3 is positive")

		result, err = expr.Eval(ctx, -2)
		require.NoError(t, err)
		assert.True(t, result, "-2 is even")

		result, err = expr.Eval(ctx, -3)
		require.NoError(t, err)
		assert.False(t, result, "-3 is neither positive nor even")
	})
}

// Mock implementation of BoolExpr for testing
type mockBoolExpr[K any] struct {
	evalFunc func(ctx context.Context, tCtx K) (bool, error)
}

func (m *mockBoolExpr[K]) Eval(ctx context.Context, tCtx K) (bool, error) {
	return m.evalFunc(ctx, tCtx)
}
