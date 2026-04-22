# 简化全局 CSS 设计 Spec

## Why
目前的全局 CSS 设计包含过多的渐变、发光效果、阴影和复杂的动画（例如全息故障效果等），导致页面显得过于花哨。用户希望简化这些视觉元素，在保持科技感的同时让界面更加简洁、现代。

## What Changes
- 移除或简化 `style.css` 和 `tokens.css` 中过于复杂的 `radial-gradient` 和多层背景渐变。
- 移除导致“花哨”的干扰动画（例如故障动画 `textGlitch` 等）。
- 减少过度的发光阴影（box-shadow）和模糊效果（backdrop-filter、blur），使边界和面板更清晰锐利。
- 统一并精简色彩层级，保留深色背景和高对比度点缀色（如青色/蓝色），保持“简洁的科技感”。

## Impact
- Affected specs: UI 视觉呈现
- Affected code: `app/static/style.css`, `app/static/tokens.css` 及可能的其他关联 CSS 文件。

## MODIFIED Requirements
### Requirement: 简化背景和装饰性元素
系统应该使用更少层级的渐变和发光背景。移除不必要的发光圈（`.screen-aura`, `.screen-noise`, `.cursor-glow`）及全息干扰特效。

### Requirement: 简化面板和卡片样式
卡片和面板的边框和背景应更加克制，减少透明度和过度模糊，避免视觉干扰，使内容更易读。

### Requirement: 统一按钮和交互态
交互元素的悬停效果应当简洁有力，去除过度夸张的文本阴影和位移动画。
