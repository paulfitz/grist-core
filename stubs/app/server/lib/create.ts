import { makeSimpleCreator } from 'app/server/lib/ICreate';
import {shell} from 'electron';

export const create = makeSimpleCreator({
  sessionSecret: 'Phoo2ag1jaiz6Moo2Iese2xoaphahbai3oNg7diemohlah0ohtae9iengafieS2Hae7quungoCi9iaPh',
  Shell: () => { return shell as any; }
});
