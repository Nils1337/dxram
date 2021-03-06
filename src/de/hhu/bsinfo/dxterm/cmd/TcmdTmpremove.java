/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxterm.cmd;

import java.util.Collections;
import java.util.List;

import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;
import de.hhu.bsinfo.dxterm.AbstractTerminalCommand;
import de.hhu.bsinfo.dxterm.TerminalCommandString;
import de.hhu.bsinfo.dxterm.TerminalServerStdin;
import de.hhu.bsinfo.dxterm.TerminalServerStdout;
import de.hhu.bsinfo.dxterm.TerminalServiceAccessor;

/**
 * Remove a (stored) chunk from temporary storage (superpeer storage)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.04.2017
 */
public class TcmdTmpremove extends AbstractTerminalCommand {
    public TcmdTmpremove() {
        super("tmpremove");
    }

    @Override
    public String getHelp() {
        return "Remove a (stored) chunk from temporary storage (superpeer storage)\n" + "Usage: tmpremove <id>\n" +
                "  id: Id of the chunk in temporary storage";
    }

    @Override
    public void exec(final TerminalCommandString p_cmd, final TerminalServerStdout p_stdout, final TerminalServerStdin p_stdin,
            final TerminalServiceAccessor p_services) {
        int id = p_cmd.getArgInt(0, -1);

        if (id == -1) {
            p_stdout.printlnErr("No id specified");
            return;
        }

        TemporaryStorageService tmpstore = p_services.getService(TemporaryStorageService.class);

        if (tmpstore.remove(id)) {
            p_stdout.printfln("Removed chunk with id 0x%X from temporary storage", id);
        } else {
            p_stdout.printlnErr("Creating chunk in temporary storage failed");
        }
    }

    @Override
    public List<String> getArgumentCompletionSuggestions(final int p_argumentPos, final TerminalCommandString p_cmdStr,
            final TerminalServiceAccessor p_services) {
        return Collections.emptyList();
    }
}
