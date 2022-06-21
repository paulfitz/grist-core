import {Computed, dom, fromKo, input, makeTestId, onElem, styled, TestId} from 'grainjs';

import * as commands from 'app/client/components/commands';
import {dragOverClass} from 'app/client/lib/dom';
import {selectFiles, uploadFiles} from 'app/client/lib/uploads';
import {cssRow} from 'app/client/ui/RightPanel';
import {colors, vars} from 'app/client/ui2018/cssVars';
import {NewAbstractWidget} from 'app/client/widgets/NewAbstractWidget';
import {encodeQueryParams} from 'app/common/gutil';
import {MetaTableData} from 'app/client/models/TableData';
import {UploadResult} from 'app/common/uploads';
import {extname} from 'path';

const testId: TestId = makeTestId('test-pw-');

const attachmentWidget = styled('div.attachment_widget.field_clip', `
  display: flex;
  flex-wrap: wrap;
  white-space: pre-wrap;
`);

const attachmentIcon = styled('div.attachment_icon.glyphicon.glyphicon-paperclip', `
  position: absolute;
  top: 2px;
  left: 2px;
  padding: 2px;
  background-color: #D0D0D0;
  color: white;
  border-radius: 2px;
  border: none;
  cursor: pointer;
  box-shadow: 0 0 0 1px white;
  z-index: 1;

  &:hover {
    background-color: #3290BF;
  }
`);

const attachmentPreview = styled('div', `
  color: black;
  background-color: var(--grist-color-white);
  border: 1px solid #bbb;
  margin: 0 2px 2px 0;
  position: relative;
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 0;
  &:hover {
    border-color: ${colors.lightGreen};
  }
`);

const sizeLabel = styled('div', `
  color: ${colors.slate};
  margin-right: 9px;
`);

export interface SavingObservable<T> extends ko.Observable<T> {
  setAndSave(value: T): void;
}

/**
 * AttachmentsWidget - A widget for displaying attachments as image previews.
 */
export class AttachmentsWidget extends NewAbstractWidget {

  private _attachmentsTable: MetaTableData<'_grist_Attachments'>;
  private _height: SavingObservable<string>;

  constructor(field: any) {
    super(field);

    // TODO: the Attachments table currently treated as metadata, and loaded on open,
    // but should probably be loaded on demand as it contains user data, which may be large.
    this._attachmentsTable = this._getDocData().getMetaTable('_grist_Attachments');

    this._height = this.options.prop('height') as SavingObservable<string>;

    this.autoDispose(this._height.subscribe(() => {
      this.field.viewSection().events.trigger('rowHeightChange');
    }));
  }

  public buildDom(_row: any): Element {
    // NOTE: A cellValue of the correct type includes the list encoding designator 'L' as the
    // first element.
    const cellValue: SavingObservable<number[]> = _row[this.field.colId()];
    const values = Computed.create(null, fromKo(cellValue), (use, _cellValue) =>
      Array.isArray(_cellValue) ? _cellValue.slice(1) : []);

    return attachmentWidget(
      dom.autoDispose(values),

      dragOverClass('attachment_drag_over'),
      attachmentIcon(
        dom.cls('attachment_hover_icon', (use) => use(values).length > 0),
        dom.on('click', () => this._selectAndSave(cellValue))
      ),
      dom.forEach(values, (value: number) =>
        isNaN(value) ? null : this._buildAttachment(value, values)
      ),
      dom.on('drop', ev => this._uploadAndSave(cellValue, ev.dataTransfer!.files))
    );
  }

  public buildConfigDom(): Element {
    const inputRange = input(fromKo(this._height), {onInput: true}, {
      style: 'margin: 0 5px;',
      type: 'range',
      min: '16',
      max: '96',
      value: '36'
    }, testId('thumbnail-size'));
    // Save the height on change event (when the user releases the drag button)
    onElem(inputRange, 'change', (ev: any) => { this._height.setAndSave(ev.target.value); });
    return cssRow(
      sizeLabel('Size'),
      inputRange
    );
  }

  protected _buildAttachment(value: number, allValues: Computed<number[]>): Element {
    const filename = this._attachmentsTable.getValue(value, 'fileName')!;
    const fileIdent = this._attachmentsTable.getValue(value, 'fileIdent')!;
    const height = this._attachmentsTable.getValue(value, 'imageHeight')!;
    const width = this._attachmentsTable.getValue(value, 'imageWidth')!;
    const hasPreview = Boolean(height);
    const ratio = hasPreview ? (width / height) : 1;

    return attachmentPreview({title: filename}, // Add a filename tooltip to the previews.
      dom.style('height', (use) => `${use(this._height)}px`),
      dom.style('width', (use) => `${parseInt(use(this._height), 10) * ratio}px`),
      // TODO: Update to legitimately determine whether a file preview exists.
      hasPreview ? dom('img', {style: 'height: 100%; min-width: 100%; vertical-align: top;'},
        dom.attr('src', this._getUrl(value))
      ) : renderFileType(filename, fileIdent, this._height),
      // Open editor as if with input, using it to tell it which of the attachments to show. We
      // pass in a 1-based index. Hitting a key opens the cell, and this approach allows an
      // accidental feature of opening e.g. second attachment by hitting "2".
      dom.on('dblclick', () => commands.allCommands.input.run(String(allValues.get().indexOf(value) + 1))),
      testId('thumbnail'),
    );
  }

  // Returns the attachment download url.
  private _getUrl(rowId: number): string {
    const ident = this._attachmentsTable.getValue(rowId, 'fileIdent');
    if (!ident) {
      return '';
    } else {
      const docComm = this._getDocComm();
      return docComm.docUrl('attachment') + '?' + encodeQueryParams({
        ...docComm.getUrlParams(),
        ident,
        name: this._attachmentsTable.getValue(rowId, 'fileName')
      });
    }
  }

  private async _selectAndSave(value: SavingObservable<number[]>): Promise<void> {
    const uploadResult = await selectFiles({docWorkerUrl: this._getDocComm().docWorkerUrl,
                                            multiple: true, sizeLimit: 'attachment'});
    return this._save(value, uploadResult);
  }

  private async _uploadAndSave(value: SavingObservable<number[]>, files: FileList): Promise<void> {
    const uploadResult = await uploadFiles(Array.from(files),
                                           {docWorkerUrl: this._getDocComm().docWorkerUrl,
                                            sizeLimit: 'attachment'});
    return this._save(value, uploadResult);
  }

  private async _save(value: SavingObservable<number[]>, uploadResult: UploadResult|null): Promise<void> {
    if (!uploadResult) { return; }
    const rowIds = await this._getDocComm().addAttachments(uploadResult.uploadId);
    // Values should be saved with a leading "L" to fit Grist's list value encoding.
    const formatted: any[] = value() ? value() : ["L"];
    value.setAndSave(formatted.concat(rowIds));
    // Trigger a row height change in case the added attachment wraps to the next line.
    this.field.viewSection().events.trigger('rowHeightChange');
  }
}

export function renderFileType(fileName: string, fileIdent: string, height?: ko.Observable<string>): HTMLElement {
  // Prepend 'x' to ensure we return the extension even if the basename is empty (e.g. ".xls").
  // Take slice(1) to strip off the leading period.
  const extension = extname('x' + fileName).slice(1) || extname('x' + fileIdent).slice(1) || '?';
  return cssFileType(extension.toUpperCase(),
    height && cssFileType.cls((use) => {
      const size = parseFloat(use(height));
      return size < 28 ? '-small' : size < 60 ? '-medium' : '-large';
    }),
  );
}

const cssFileType = styled('div', `
  height: 100%;
  width: 100%;
  max-height: 80px;
  max-width: 80px;
  background-color: ${colors.slate};
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: ${vars.mediumFontSize};
  font-weight: bold;
  color: white;
  overflow: hidden;

  &-small { font-size: ${vars.xxsmallFontSize}; }
  &-medium { font-size: ${vars.smallFontSize}; }
  &-large { font-size: ${vars.mediumFontSize}; }
`);
