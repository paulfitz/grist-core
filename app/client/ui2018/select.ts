import {colors, vars} from 'app/client/ui2018/cssVars';
import {styled} from 'grainjs';

// Import popweasel so that the styles we define here are included later in CSS, and take priority
// over popweasel styles, when used together.
import 'popweasel';

/**
 * Style for a select dropdown button.
 *
 * This incorporates styling from popweasel's select, so that it can be used to style buttons that
 * don't use it.
 */
export const cssSelectBtn = styled('div', `
  position: relative;
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: 100%;
  height: 30px;
  line-height: 16px;
  background-color: var(--grist-color-white);
  color: ${colors.dark};
  --icon-color: ${colors.dark};
  font-size: ${vars.mediumFontSize};
  padding: 5px;
  border: 1px solid ${colors.darkGrey};
  border-radius: 3px;
  cursor: pointer;
  overflow: hidden;
  white-space: nowrap;
  text-overflow: ellipsis;
  -webkit-appearance: none;
  -moz-appearance: none;
  user-select: none;
  -moz-user-select: none;
  outline: none;

  &:focus {
    outline: none;
    box-shadow: 0px 0px 2px 2px #5E9ED6;
  }

  &.disabled {
    color: grey;
    cursor: pointer;
  }
`);
