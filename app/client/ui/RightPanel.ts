/**
 * Builds the structure of the right-side panel containing configuration and assorted tools.
 * It includes the regular tabs, to configure the Page (including several sub-tabs), and Field;
 * and allows other tools, such as Activity Feed, to be rendered temporarily in its place.
 *
 * A single RightPanel object is created in AppUI for a document page, and attached to PagePanels.
 * GristDoc registers callbacks with it to create various standard tabs. These are created as
 * needed, and destroyed when hidden.
 *
 * In addition, tools such as "Activity Feed" may use openTool() to replace the panel header and
 * content. The user may dismiss this panel.
 *
 * All methods above return an object which may  be disposed to close and dispose that specific
 * tab from the outside (e.g. when GristDoc is disposed).
 */

import * as commands from 'app/client/components/commands';
import {GristDoc, IExtraTool, TabContent} from 'app/client/components/GristDoc';
import * as RefSelect from 'app/client/components/RefSelect';
import * as ViewConfigTab from 'app/client/components/ViewConfigTab';
import {domAsync} from 'app/client/lib/domAsync';
import * as imports from 'app/client/lib/imports';
import {createSessionObs} from 'app/client/lib/sessionObs';
import {reportError} from 'app/client/models/AppModel';
import {ColumnRec, ViewSectionRec} from 'app/client/models/DocModel';
import {GridOptions} from 'app/client/ui/GridOptions';
import {attachPageWidgetPicker, IPageWidget, toPageWidget} from 'app/client/ui/PageWidgetPicker';
import {linkFromId, linkId, selectBy} from 'app/client/ui/selectBy';
import {CustomSectionConfig} from 'app/client/ui/CustomSectionConfig';
import {VisibleFieldsConfig} from 'app/client/ui/VisibleFieldsConfig';
import {IWidgetType, widgetTypes} from 'app/client/ui/widgetTypes';
import {basicButton, primaryButton} from 'app/client/ui2018/buttons';
import {colors, testId, vars} from 'app/client/ui2018/cssVars';
import {textInput} from 'app/client/ui2018/editableLabel';
import {IconName} from 'app/client/ui2018/IconList';
import {icon} from 'app/client/ui2018/icons';
import {select} from 'app/client/ui2018/menus';
import {FieldBuilder} from 'app/client/widgets/FieldBuilder';
import {StringUnion} from 'app/common/StringUnion';
import {bundleChanges, Computed, Disposable, dom, domComputed, DomContents,
        DomElementArg, DomElementMethod, IDomComponent} from 'grainjs';
import {MultiHolder, Observable, styled, subscribe} from 'grainjs';
import * as ko from 'knockout';

// Represents a top tab of the right side-pane.
const TopTab = StringUnion("pageWidget", "field");

// Represents a subtab of pageWidget in the right side-pane.
const PageSubTab = StringUnion("widget", "sortAndFilter", "data");

// A map of widget type to the icon and label to use for a field of that widget.
const fieldTypes = new Map<IWidgetType, {label: string, icon: IconName, pluralLabel: string}>([
  ['record', {label: 'Column', icon: 'TypeCell', pluralLabel: 'Columns'}],
  ['detail', {label: 'Field', icon: 'TypeCell', pluralLabel: 'Fields'}],
  ['single', {label: 'Field', icon: 'TypeCell', pluralLabel: 'Fields'}],
  ['chart', {label: 'Series', icon: 'ChartLine', pluralLabel: 'Series'}],
  ['custom', {label: 'Column', icon: 'TypeCell', pluralLabel: 'Columns'}],
]);

// Returns the icon and label of a type, default to those associate to 'record' type.
export function getFieldType(widgetType: IWidgetType|null) {
  return fieldTypes.get(widgetType || 'record') || fieldTypes.get('record')!;
}

export class RightPanel extends Disposable {
  public readonly header: DomContents;
  public readonly content: DomContents;

  // If the panel is showing a tool, such as Action Log, instead of the usual section/field
  // configuration, this will be set to the tool's header and content.
  private _extraTool: Observable<IExtraTool|null>;

  // Which of the two standard top tabs (page widget or field) is selected, or was last selected.
  private _topTab = createSessionObs(this, "rightTopTab", "pageWidget", TopTab.guard);

  // Which subtab is open for configuring page widget.
  private _subTab = createSessionObs(this, "rightPageSubTab", "widget", PageSubTab.guard);

  // Which type of page widget is active, e.g. "record" or "chart". This affects the names and
  // icons in the top tab.
  private _pageWidgetType = Computed.create<IWidgetType|null>(this, (use) => {
    const section: ViewSectionRec = use(this._gristDoc.viewModel.activeSection);
    return (use(section.parentKey) || null) as IWidgetType;
  });

  // Returns the active section if it's valid, null otherwise.
  private _validSection = Computed.create(this, (use) => {
    const sec = use(this._gristDoc.viewModel.activeSection);
    return sec.getRowId() ? sec : null;
  });

  constructor(private _gristDoc: GristDoc, private _isOpen: Observable<boolean>) {
    super();
    this._extraTool = _gristDoc.rightPanelTool;
    this.autoDispose(subscribe(this._extraTool, (_use, tool) => tool && _isOpen.set(true)));
    this.header = this._buildHeaderDom();
    this.content = this._buildContentDom();

    this.autoDispose(commands.createGroup({
      fieldTabOpen: () => this._openFieldTab(),
      viewTabOpen: () => this._openViewTab(),
      sortFilterTabOpen: () => this._openSortFilter(),
      dataSelectionTabOpen: () => this._openDataSelection()
    }, this, true));
  }

  private _openFieldTab() {
    this._open('field');
  }

  private _openViewTab() {
    this._open('pageWidget', 'widget');
  }

  private _openSortFilter() {
    this._open('pageWidget', 'sortAndFilter');
  }

  private _openDataSelection() {
    this._open('pageWidget', 'data');
  }

  private _open(topTab: typeof TopTab.type, subTab?: typeof PageSubTab.type) {
    bundleChanges(() => {
      this._isOpen.set(true);
      this._topTab.set(topTab);
      if (subTab) {
        this._subTab.set(subTab);
      }
    });
  }

  private _buildHeaderDom() {
    return dom.domComputed((use) => {
      if (!use(this._isOpen)) { return null; }
      const tool = use(this._extraTool);
      return tool ? this._buildToolHeader(tool) : this._buildStandardHeader();
    });
  }

  private _buildToolHeader(tool: IExtraTool) {
    return cssTopBarItem(cssTopBarIcon(tool.icon), tool.label,
      cssHoverCircle(cssHoverIcon("CrossBig"),
        dom.on('click', () => this._gristDoc.showTool('none')),
        testId('right-tool-close'),
      ),
      cssTopBarItem.cls('-selected', true)
    );
  }

  private _buildStandardHeader() {
    return dom.maybe(this._pageWidgetType, (type) => {
      const widgetInfo = widgetTypes.get(type) || {label: 'Table', icon: 'TypeTable'};
      const fieldInfo = getFieldType(type);
      return [
        cssTopBarItem(cssTopBarIcon(widgetInfo.icon), widgetInfo.label,
          cssTopBarItem.cls('-selected', (use) => use(this._topTab) === 'pageWidget'),
          dom.on('click', () => this._topTab.set("pageWidget")),
          testId('right-tab-pagewidget')),
        cssTopBarItem(cssTopBarIcon(fieldInfo.icon), fieldInfo.label,
          cssTopBarItem.cls('-selected', (use) => use(this._topTab) === 'field'),
          dom.on('click', () => this._topTab.set("field")),
          testId('right-tab-field')),
      ];
    });
  }

  private _buildContentDom() {
    return dom.domComputed((use) => {
      if (!use(this._isOpen)) { return null; }
      const tool = use(this._extraTool);
      if (tool) { return tabContentToDom(tool.content); }

      const topTab = use(this._topTab);
      if (topTab === 'field') {
        return dom.create(this._buildFieldContent.bind(this));
      }
      if (topTab === 'pageWidget' && use(this._pageWidgetType)) {
        return dom.create(this._buildPageWidgetContent.bind(this));
      }
      return null;
    });
  }

  private _buildFieldContent(owner: MultiHolder) {
    const fieldBuilder = owner.autoDispose(ko.computed(() => {
      const vsi = this._gristDoc.viewModel.activeSection?.().viewInstance();
      return vsi && vsi.activeFieldBuilder();
    }));

    const docModel = this._gristDoc.docModel;
    const origColRef = owner.autoDispose(ko.computed(() => fieldBuilder()?.origColumn.origColRef() || 0));
    const origColumn = owner.autoDispose(docModel.columns.createFloatingRowModel(origColRef));
    const isColumnValid = owner.autoDispose(ko.computed(() => Boolean(origColRef())));

    // Builder for the reference display column multiselect.
    const refSelect = owner.autoDispose(RefSelect.create({docModel, origColumn, fieldBuilder}));

    // build cursor position observable
    const cursor = owner.autoDispose(ko.computed(() => {
      const vsi = this._gristDoc.viewModel.activeSection?.().viewInstance();
      return vsi?.cursor.currentPosition() ?? {};
    }));

    return domAsync(imports.loadViewPane().then(ViewPane => {
      const {buildNameConfig, buildFormulaConfig} = ViewPane.FieldConfig;
      return dom.maybe(isColumnValid, () =>
        buildConfigContainer(
          dom.create(buildNameConfig, origColumn, cursor),
          cssSeparator(),
          dom.create(buildFormulaConfig, origColumn, this._gristDoc, this._activateFormulaEditor.bind(this)),
          cssSeparator(),
          cssLabel('COLUMN TYPE'),
          dom.maybe<FieldBuilder|null>(fieldBuilder, builder => [
            builder.buildSelectTypeDom(),
            builder.buildSelectWidgetDom(),
            builder.buildConfigDom()
          ]),
          cssSeparator(),
          dom.maybe(refSelect.isForeignRefCol, () => [
            cssLabel('Add referenced columns'),
            cssRow(refSelect.buildDom()),
            cssSeparator()
          ]),
          cssLabel('TRANSFORM'),
          dom.maybe<FieldBuilder|null>(fieldBuilder, builder => builder.buildTransformDom()),
          this._disableIfReadonly(),
        )
      );
    }));
  }

  // Helper to activate the side-pane formula editor over the given HTML element.
  private _activateFormulaEditor(
    // Element to attach to.
    refElem: Element,
    // Simulate user typing on the cell - open editor with an initial value.
    editValue?: string,
    // Custom save handler.
    onSave?: (column: ColumnRec, formula: string) => Promise<void>,
    // Custom cancel handler.
    onCancel?: () => void,) {
    const vsi = this._gristDoc.viewModel.activeSection().viewInstance();
    if (!vsi) { return; }
    const editRowModel = vsi.moveEditRowToCursor();
    return vsi.activeFieldBuilder.peek().openSideFormulaEditor(editRowModel, refElem, editValue, onSave, onCancel);
  }

  private _buildPageWidgetContent(_owner: MultiHolder) {
    return [
      cssSubTabContainer(
        cssSubTab('Widget',
          cssSubTab.cls('-selected', (use) => use(this._subTab) === 'widget'),
          dom.on('click', () => this._subTab.set("widget")),
          testId('config-widget')),
        cssSubTab('Sort & Filter',
          cssSubTab.cls('-selected', (use) => use(this._subTab) === 'sortAndFilter'),
          dom.on('click', () => this._subTab.set("sortAndFilter")),
          testId('config-sortAndFilter')),
        cssSubTab('Data',
          cssSubTab.cls('-selected', (use) => use(this._subTab) === 'data'),
          dom.on('click', () => this._subTab.set("data")),
          testId('config-data')),
      ),
      dom.domComputed(this._subTab, (subTab) => (
        dom.maybe(this._validSection, (activeSection) => (
          buildConfigContainer(
            subTab === 'widget' ? dom.create(this._buildPageWidgetConfig.bind(this), activeSection) :
              subTab === 'sortAndFilter' ? dom.create(this._buildPageSortFilterConfig.bind(this)) :
              subTab === 'data' ? dom.create(this._buildPageDataConfig.bind(this), activeSection) :
              null
          )
        ))
      ))
    ];
  }

  private _createViewConfigTab(owner: MultiHolder): Observable<null|ViewConfigTab> {
    const viewConfigTab = Observable.create<null|ViewConfigTab>(owner, null);
    const gristDoc = this._gristDoc;
    imports.loadViewPane()
      .then(ViewPane => {
        if (owner.isDisposed()) { return; }
        viewConfigTab.set(owner.autoDispose(
          ViewPane.ViewConfigTab.create({gristDoc, viewModel: gristDoc.viewModel})));
      })
      .catch(reportError);
    return viewConfigTab;
  }

  private _buildPageWidgetConfig(owner: MultiHolder, activeSection: ViewSectionRec) {
    // TODO: This uses private methods from ViewConfigTab. These methods are likely to get
    // refactored, but if not, should be made public.
    const viewConfigTab = this._createViewConfigTab(owner);
    const hasCustomMapping = Computed.create(owner, use => {
      const isCustom = use(this._pageWidgetType) === 'custom';
      const hasColumnMapping = use(activeSection.columnsToMap);
      return Boolean(isCustom && hasColumnMapping);
    });
    return dom.maybe(viewConfigTab, (vct) => [
      this._disableIfReadonly(),
      cssLabel(dom.text(use => use(activeSection.isRaw) ? 'DATA TABLE NAME' : 'WIDGET TITLE'),
               dom.style('margin-bottom', '14px')),
      cssRow(cssTextInput(
        Computed.create(owner, (use) => use(activeSection.titleDef)),
        val => activeSection.titleDef.saveOnly(val),
        testId('right-widget-title')
      )),

      dom.maybe(
        (use) => !use(activeSection.isRaw),
        () => cssRow(
          primaryButton('Change Widget', this._createPageWidgetPicker()),
          cssRow.cls('-top-space')
        ),
      ),

      cssSeparator(),

      dom.maybe((use) => ['detail', 'single'].includes(use(this._pageWidgetType)!), () => [
        cssLabel('Theme'),
        dom('div',
          vct._buildThemeDom(),
          vct._buildLayoutDom())
      ]),

      domComputed((use) => {
        if (use(this._pageWidgetType) !== 'record') { return null; }
        return dom.create(GridOptions, activeSection);
      }),

      dom.maybe((use) => use(this._pageWidgetType) === 'chart', () => [
        cssLabel('CHART TYPE'),
        vct._buildChartConfigDom(),
      ]),

      dom.maybe((use) => use(this._pageWidgetType) === 'custom', () => {
        const parts = vct._buildCustomTypeItems() as any[];
        return [
          cssLabel('CUSTOM'),
          // If 'customViewPlugin' feature is on, show the toggle that allows switching to
          // plugin mode. Note that the default mode for a new 'custom' view is 'url', so that's
          // the only one that will be shown without the feature flag.
          dom.maybe((use) => use(this._gristDoc.app.features).customViewPlugin,
            () => dom('div', parts[0].buildDom())),
          dom.maybe(use => use(activeSection.customDef.mode) === 'plugin',
            () => dom('div', parts[2].buildDom())),
          // In the default url mode, allow picking a url and granting/forbidding
          // access to data.
          dom.maybe(use => use(activeSection.customDef.mode) === 'url',
            () => dom.create(CustomSectionConfig, activeSection, this._gristDoc)),
        ];
      }),

      dom.maybe(
        (use) => !(
          use(hasCustomMapping) ||
          use(this._pageWidgetType) === 'chart' ||
          use(activeSection.isRaw)
        ),
        () => [
          cssSeparator(),
          dom.create(VisibleFieldsConfig, this._gristDoc, activeSection),
        ]),
    ]);
  }

  private _buildPageSortFilterConfig(owner: MultiHolder) {
    const viewConfigTab = this._createViewConfigTab(owner);
    return [
      cssLabel('SORT'),
      dom.maybe(viewConfigTab, (vct) => vct.buildSortDom()),
      cssSeparator(),

      cssLabel('FILTER'),
      dom.maybe(viewConfigTab, (vct) => dom('div', vct._buildFilterDom())),
    ];
  }

  private _buildPageDataConfig(owner: MultiHolder, activeSection: ViewSectionRec) {
    const viewConfigTab = this._createViewConfigTab(owner);
    const viewModel = this._gristDoc.viewModel;
    const table = activeSection.table;
    const groupedBy = Computed.create(owner, (use) => use(use(table).groupByColumns));
    const link = Computed.create(owner, (use) => {
      return linkId({
        srcSectionRef: use(activeSection.linkSrcSectionRef),
        srcColRef: use(activeSection.linkSrcColRef),
        targetColRef: use(activeSection.linkTargetColRef)
      });
    });

    // TODO: this computed is not enough to make sure that the linkOptions are up to date. Indeed
    // the selectBy function depends on a much greater number of observables. Creating that many
    // dependencies does not seem a better approach. Instead, we could refresh the list of
    // linkOptions only when the user clicks the dropdown. Such behaviour is not supported by the
    // weasel select function as of writing and would require a custom implementation.
    const linkOptions = Computed.create(owner, (use) =>
      selectBy(
        this._gristDoc.docModel,
        use(viewModel.viewSections).peek(),
        activeSection,
      )
    );

    link.onWrite((val) => this._gristDoc.saveLink(linkFromId(val)));
    return [
      this._disableIfReadonly(),
      cssLabel('DATA TABLE'),
      cssRow(
        cssIcon('TypeTable'), cssDataLabel('SOURCE DATA'),
        cssContent(dom.text((use) => use(use(table).primaryTableId)),
                   testId('pwc-table'))
      ),
      dom(
        'div',
        cssRow(cssIcon('Pivot'), cssDataLabel('GROUPED BY')),
        cssRow(domComputed(groupedBy, (cols) => cssList(cols.map((c) => (
          cssListItem(dom.text(c.label),
                      testId('pwc-groupedBy-col'))
        ))))),

        testId('pwc-groupedBy'),
        // hide if not a summary table
        dom.hide((use) => !use(use(table).summarySourceTable)),
      ),

      dom.maybe((use) => !use(activeSection.isRaw), () =>
        cssButtonRow(primaryButton('Edit Data Selection', this._createPageWidgetPicker(),
          testId('pwc-editDataSelection')),
          dom.maybe(
            use => Boolean(use(use(activeSection.table).summarySourceTable)),
            () => basicButton(
              'Detach',
              dom.on('click', () => this._gristDoc.docData.sendAction(
                ["DetachSummaryViewSection", activeSection.getRowId()])),
              testId('detach-button'),
            )),
          cssRow.cls('-top-space'),
      )),

      // TODO: "Advanced settings" is for "on-demand" marking of tables. This should only be shown
      // for raw data tables (once that's supported), should have updated UI, and should possibly
      // be hidden for free plans.
      dom.maybe(viewConfigTab, (vct) => cssRow(
        dom('div', vct._buildAdvancedSettingsDom()),
      )),
      cssSeparator(),

      dom.maybe((use) => !use(activeSection.isRaw), () => [
        cssLabel('SELECT BY'),
        cssRow(
          select(link, linkOptions, {defaultLabel: 'Select Widget'}),
          testId('right-select-by')
        ),
      ]),

      domComputed((use) => {
        const activeSectionRef = activeSection.getRowId();
        const allViewSections = use(use(viewModel.viewSections).getObservable());
        const selectorFor = allViewSections.filter((sec) => use(sec.linkSrcSectionRef) === activeSectionRef);
        // TODO: sections should be listed following the order of appearance in the view layout (ie:
        // left/right - top/bottom);
        return selectorFor.length ? [
          cssLabel('SELECTOR FOR', testId('selector-for')),
          cssRow(cssList(selectorFor.map((sec) => this._buildSectionItem(sec))))
        ] : null;
      }),
    ];
  }

  private _createPageWidgetPicker(): DomElementMethod {
    const gristDoc = this._gristDoc;
    const section = gristDoc.viewModel.activeSection;
    const onSave = (val: IPageWidget) => gristDoc.saveViewSection(section.peek(), val);
    return (elem) => { attachPageWidgetPicker(elem, gristDoc.docModel, onSave, {
      buttonLabel:  'Save',
      value: () => toPageWidget(section.peek()),
      selectBy: (val) => gristDoc.selectBy(val),
    }); };
  }

  // Returns dom for a section item.
  private _buildSectionItem(sec: ViewSectionRec) {
    return cssListItem(
      dom.text(sec.titleDef),
      testId('selector-for-entry')
    );
  }

  // Returns a DomArg that disables the content of the panel by adding a transparent overlay on top
  // of it.
  private _disableIfReadonly() {
    if (this._gristDoc.docPageModel) {
      return dom.maybe(this._gristDoc.docPageModel.isReadonly,  () => (
        cssOverlay(
          testId('disable-overlay'),
          cssBottomText('You do not have edit access to this document'),
        )
      ));
    }
  }
}

export function buildConfigContainer(...args: DomElementArg[]): HTMLElement {
  return cssConfigContainer(
    // The `position: relative;` style is needed for the overlay for the readonly mode. Note that
    // we cannot set it on the cssConfigContainer directly because it conflicts with how overflow
    // works. `padding-top: 1px;` prevents collapsing the top margins for the container and the
    // first child.
    dom('div', {style: 'position: relative; padding-top: 1px;'}, ...args),
  );
}

// This logic is copied from SidePane.js for building DOM from TabContent.
// TODO It may not be needed after new-ui refactoring of the side-pane content.
function tabContentToDom(content: Observable<TabContent[]>|TabContent[]|IDomComponent) {
  function buildItemDom(item: any) {
    return dom('div.config_item',
      dom.show(item.showObs || true),
      item.buildDom()
    );
  }

  if ("buildDom" in content) {
    return content.buildDom();
  }

  return cssTabContents(
    dom.forEach(content, itemOrHeader => {
      if (itemOrHeader.header) {
        return dom('div.config_group',
          dom.show(itemOrHeader.showObs || true),
          itemOrHeader.label ? dom('div.config_header', itemOrHeader.label) : null,
          dom.forEach(itemOrHeader.items, item => buildItemDom(item)),
        );
      } else {
        return buildItemDom(itemOrHeader);
      }
    })
  );
}

const cssOverlay = styled('div', `
  background-color: var(--grist-color-white);
  opacity: 0.8;
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  z-index: 100;
`);

const cssBottomText = styled('span', `
  position: absolute;
  bottom: -40px;
  padding: 4px 16px;
`);

export const cssLabel = styled('div', `
  text-transform: uppercase;
  margin: 16px 16px 12px 16px;
  font-size: ${vars.xsmallFontSize};
`);

// Additional text in label (greyed out)
export const cssSubLabel = styled('span', `
  text-transform: none;
  font-size: ${vars.xsmallFontSize};
  color: ${colors.slate};
`);

export const cssRow = styled('div', `
  display: flex;
  margin: 8px 16px;
  align-items: center;
  &-top-space {
    margin-top: 24px;
  }
  &-disabled {
    color: ${colors.slate};
  }
`);

export const cssBlockedCursor = styled('span', `
  &, & * {
    cursor: not-allowed !important;
  }
`);

export const cssButtonRow = styled(cssRow, `
  margin-left: 0;
  margin-right: 0;
  & > button {
    margin-left: 16px;
  }
`);

export const cssIcon = styled(icon, `
  flex: 0 0 auto;
  --icon-color: ${colors.slate};
`);

const cssTopBarItem = styled('div', `
  flex: 1 1 0px;
  height: 100%;
  background-color: ${colors.lightGrey};
  font-weight: ${vars.headerControlTextWeight};
  color: ${colors.dark};
  --icon-color: ${colors.slate};
  display: flex;
  align-items: center;
  cursor: default;

  &-selected {
    background-color: ${colors.lightGreen};
    font-weight: initial;
    color: ${colors.light};
    --icon-color: ${colors.light};
  }
  &:not(&-selected):hover {
    background-color: ${colors.mediumGrey};
    --icon-color: ${colors.lightGreen};
  }
`);

const cssTopBarIcon = styled(icon, `
  flex: none;
  margin: 16px;
  height: 16px;
  width: 16px;
  background-color: vars(--icon-color);
`);

const cssHoverCircle = styled('div', `
  margin-left: auto;
  margin-right: 8px;
  width: 32px;
  height: 32px;
  background: none;
  border-radius: 16px;
  display: flex;
  align-items: center;
  justify-content: center;

  &:hover {
    background-color: ${colors.darkGreen};
  }
`);

const cssHoverIcon = styled(icon, `
  height: 16px;
  width: 16px;
  background-color: vars(--icon-color);
`);

export const cssSubTabContainer = styled('div', `
  height: 48px;
  flex: none;
  display: flex;
  align-items: center;
  justify-content: space-between;
`);

export const cssSubTab = styled('div', `
  color: ${colors.lightGreen};
  flex: auto;
  height: 100%;
  display: flex;
  flex-direction: column;
  justify-content: flex-end;
  text-align: center;
  padding-bottom: 8px;
  border-bottom: 1px solid ${colors.mediumGrey};
  cursor: default;

  &-selected {
    color: ${colors.dark};
    border-bottom: 1px solid ${colors.lightGreen};
  }
  &:not(&-selected):hover {
    color: ${colors.darkGreen};
  }
  &:hover {
    border-bottom: 1px solid ${colors.lightGreen};
  }
  .${cssSubTabContainer.className}:hover > &-selected:not(:hover) {
    border-bottom: 1px solid ${colors.mediumGrey};
  }
`);

const cssTabContents = styled('div', `
  padding: 16px 8px;
  overflow: auto;
`);

export const cssSeparator = styled('div', `
  border-bottom: 1px solid ${colors.mediumGrey};
  margin-top: 16px;
`);

export const cssEmptySeparator = styled('div', `
  margin-top: 16px;
`);

const cssConfigContainer = styled('div', `
  overflow: auto;
  --color-list-item: none;
  --color-list-item-hover: none;

  &:after {
    content: "";
    display: block;
    height: 40px;
  }
  & .fieldbuilder_settings {
    margin: 16px 0 0 0;
  }
`);

const cssDataLabel = styled('div', `
  flex: 0 0 81px;
  color: ${colors.slate};
  font-size: ${vars.xsmallFontSize};
  margin-left: 4px;
  margin-top: 2px;
`);

const cssContent = styled('div', `
  flex: 0 1 auto;
  overflow: hidden;
  text-overflow: ellipsis;
  min-width: 1em;
`);

const cssList = styled('div', `
  list-style: none;
  width: 100%;
`);


const cssListItem = styled('li', `
  background-color: ${colors.mediumGrey};
  border-radius: 2px;
  margin-bottom: 4px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  width: 100%;
  padding: 4px 8px;
`);

export const cssTextInput = styled(textInput, `
  flex: 1 0 auto;
`);
